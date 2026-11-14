//! The mover under deterministic simulation.
//!
//! The real mover runs here: the job store and claims ([`MetaMover`],
//! [`JobRuntime`]) on the simulated etcd, the engine, and the production
//! map commit ([`TenantMapIo`]) on the tenant map. Only MariaDB is
//! a model: one source cluster and one target cluster whose tables are
//! `(k BIGINT PRIMARY KEY, v VARCHAR)`, a GTID binlog of row images,
//! watermarks and DDL, a rename fence, position rows with fencing tokens,
//! and purging.
//!
//! Around the job: clients that route by the map like gateways do (they
//! honour holds, some late or with a stale map, and retry after a fence
//! error), DDL on the source (refused while fenced), and faults: workers
//! crashed at every job state and cutover step, etcd partitions (lost
//! leases, ambiguous commits), dropped streams, failing and ambiguous
//! target commits, failing and slow fences, and purged binlogs.
//!
//! The invariants checked (the mover's invariants, as they apply to
//! single-shard tenants):
//!
//! 1. No write acknowledged to a client is lost: the final owner's rows
//!    equal the acknowledged writes.
//! 2. The target never holds a row version the source never had (checked
//!    on every target write).
//! 3. After the fence, at most one side accepts writes: no client write is
//!    acknowledged by the source after one was acknowledged by the target.
//! 4. At commit, the target equals the (fenced) source.
//! 5. Epochs only increase, by exactly one per committed move.
//! 6. A committed cutover's hold ends within `mover.cutover_max_hold_ms`
//!    plus the hold grace.
//! 7. Every job ends `done` or `rolled_back`.
//! 8. The feed handoff: a feed consumer reading across the move, crashing
//!    and resuming, durably sees every committed change of the tenant
//!    exactly once, in commit order, none of the mover's copies, and no
//!    physical name ([`feed`]).

#![allow(clippy::many_single_char_names)] // keys, values, tables: k, v, t

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_binlog::{Gtid, GtidPos};
use sprig_catalog::placement::ShardMap;
use sprig_common::ids::{ClusterId, JobId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Op, Record, Tier, Txn};
use sprig_mover::HandoffPos;
use sprig_mover::io::{
    ChangeStream, CommitOutcome, Counter, Fenced, JobEnv, Load, MapIo, MapState, Released,
    SourceIo, TargetIo,
};
use sprig_mover::maps::TenantMapIo;
use sprig_mover::model::{
    ApplyOp, ColumnMeta, CopyState, PosRow, Row, RowChange, RowDigest, SourceDdl, SourceTxn,
    StreamItem, TableMeta, Value, Watermark,
};
use sprig_mover::{
    EnvFactory, JobRecord, JobRuntime, JobSpec, JobState, JobStore, MetaMover, Mover,
    MoverSettings, WorkerOpts,
};
use sprig_shard::{Placement, TenantMaps};
use tokio::sync::Notify;

use super::{SimClock, SimRng, World};
use crate::faults::Node;

mod feed;
// After-commit triggers on the same feed.
mod logic;
// The stream sequencer on the same feed.
mod streams;
// Prepared distributed-transaction branches at cutover.
pub mod xa;

/// The tenant moved.
const TENANT: TenantId = TenantId(1);
/// The shard schema.
const SCHEMA: &str = "t1_d1_g1_s1";
/// The source's GTID domain and server.
const DOMAIN: u32 = 1;
const SERVER: u32 = 101;
/// The DC.
const DC: &str = "dc-a";

/// What one run does.
#[derive(Debug, Clone)]
pub struct MoverScenario {
    /// Tables in the shard.
    pub tables: usize,
    /// Rows per table at the start.
    pub rows: i64,
    /// Concurrent clients.
    pub clients: usize,
    /// Inject faults.
    pub faults: bool,
}

impl MoverScenario {
    /// A scenario drawn from the seed: 1 to 3 tables of 0 to 40 rows, 1 to
    /// 3 clients, faults in most runs.
    #[must_use]
    pub fn random(rng: &SimRng) -> Self {
        Self {
            tables: 1 + rng.index(3),
            rows: i64::try_from(rng.below(41)).unwrap_or(0),
            clients: 1 + rng.index(3),
            faults: !rng.chance(0.1),
        }
    }
}

/// Fault rates for one run (0 when faults are off).
#[derive(Debug, Clone, Copy, Default)]
struct Rates {
    stream_drop: f64,
    source_err: f64,
    target_err_before: f64,
    target_err_after: f64,
    fence_fail: f64,
    fence_slow: f64,
}

impl Rates {
    fn draw(rng: &SimRng, on: bool) -> Self {
        if !on {
            return Self::default();
        }
        let p = |hi: f64| {
            #[allow(clippy::cast_precision_loss)]
            let x = rng.below(1000) as f64 / 1000.0;
            x * hi
        };
        Self {
            stream_drop: p(0.02),
            source_err: p(0.02),
            target_err_before: p(0.02),
            target_err_after: p(0.02),
            fence_fail: p(0.15),
            fence_slow: p(0.1),
        }
    }
}

/// One row change of the model: table, before, after.
type ModelChange = (String, Option<(i64, String)>, Option<(i64, String)>);

/// One binlog event group of the source model.
#[derive(Debug, Clone)]
enum Entry {
    Rows(Vec<ModelChange>),
    Wm(Watermark),
    Ddl(String),
}

type Rows = BTreeMap<String, BTreeMap<i64, String>>;

/// One binlog group of the target model.
#[derive(Debug, Clone)]
enum TgtEntry {
    /// A mover apply: rows and the job's position row, in one transaction.
    Apply(Vec<ModelChange>),
    /// A client's write after the cutover.
    Client(ModelChange),
    /// DDL the mover ran (table creation).
    Ddl(String),
}

/// The two clusters.
#[derive(Debug, Default)]
struct Db {
    src: Rows,
    binlog: Vec<Entry>,
    /// Entries with a sequence at or below this are purged.
    purged: u64,
    fenced: bool,
    dropped: bool,
    versions: BTreeSet<(String, i64, String)>,
    tgt: Rows,
    tgt_binlog: Vec<TgtEntry>,
    tgt_pos: Option<PosRow>,
    /// Client acknowledgements, by time: (on target, at).
    last_src_ack: Option<Duration>,
    first_tgt_ack: Option<Duration>,
    /// The last acknowledged write per key: `None` for a delete.
    acked: BTreeMap<(String, i64), Option<String>>,
    ddl_seq: u64,
    /// Prepared distributed-transaction branches on the source.
    xa: xa::XaState,
}

impl Db {
    fn seq(&self) -> u64 {
        self.binlog.len() as u64
    }

    fn pos(&self) -> GtidPos {
        if self.binlog.is_empty() {
            return GtidPos::new();
        }
        GtidPos::from_gtids([gtid(self.seq())])
    }

    fn log(&mut self, e: Entry) -> u64 {
        self.binlog.push(e);
        self.seq()
    }
}

fn gtid(seq: u64) -> Gtid {
    Gtid {
        domain: DOMAIN,
        server: SERVER,
        seq,
    }
}

fn seq_of(p: &GtidPos) -> u64 {
    p.get(DOMAIN).map_or(0, |g| g.seq)
}

fn lock(db: &Mutex<Db>) -> std::sync::MutexGuard<'_, Db> {
    db.lock().unwrap_or_else(PoisonError::into_inner)
}

fn fault(what: &str) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, format!("injected: {what}"))
}

fn table_meta(name: &str) -> TableMeta {
    TableMeta {
        schema: SCHEMA.to_owned(),
        name: name.to_owned(),
        columns: vec![
            ColumnMeta {
                name: "k".into(),
                generated: false,
                numeric: false,
            },
            ColumnMeta {
                name: "v".into(),
                generated: false,
                numeric: false,
            },
        ],
        pk: vec![0],
        create_sql: String::new(),
    }
}

fn row(k: i64, v: &str) -> Row {
    vec![Value::Int(k), Value::Bytes(v.as_bytes().to_vec())]
}

fn parse_row(r: &Row) -> Option<(i64, String)> {
    match (r.first()?, r.get(1)?) {
        (Value::Int(k), Value::Bytes(v)) => Some((*k, String::from_utf8_lossy(v).into_owned())),
        _ => None,
    }
}

fn key_of(after: Option<&Row>) -> Option<i64> {
    match after?.first()? {
        Value::Int(k) => Some(*k),
        _ => None,
    }
}

fn digest(t: &TableMeta, k: i64, v: &str) -> RowDigest {
    let r = row(k, v);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in k.to_be_bytes().iter().chain(v.as_bytes()) {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    RowDigest {
        key: t.key_of(&r),
        key_values: vec![Value::Int(k)],
        hash: h,
    }
}

/// The source model as the mover sees it.
struct SimSource {
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    world: World,
    rates: Rates,
    tables: Vec<String>,
    /// The XA variant's mode.
    xa: Option<xa::XaMode>,
}

impl SimSource {
    async fn call(&self, p: f64, what: &str) -> Result<()> {
        self.world.chaos_yield().await;
        tokio::time::sleep(Duration::from_micros(200 + self.world.rng().below(800))).await;
        if self.world.rng().chance(p) {
            self.world.record(format!("fault source {what}"));
            return Err(fault(what));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl SourceIo for SimSource {
    async fn schemas(&self) -> Result<Vec<String>> {
        Ok(vec![SCHEMA.to_owned()])
    }

    async fn tables(&self) -> Result<Vec<TableMeta>> {
        self.call(self.rates.source_err, "tables").await?;
        Ok(self.tables.iter().map(|t| table_meta(t)).collect())
    }

    async fn objects(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn triggers(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn binlog_pos(&self) -> Result<GtidPos> {
        self.call(self.rates.source_err, "binlog_pos").await?;
        Ok(lock(&self.db).pos())
    }

    async fn open_stream(
        &self,
        from: &GtidPos,
        tables: Arc<Vec<Arc<TableMeta>>>,
    ) -> Result<Box<dyn ChangeStream>> {
        self.call(self.rates.source_err, "open_stream").await?;
        let db = lock(&self.db);
        let s = seq_of(from);
        if s < db.purged || s > db.seq() {
            return Err(SprigError::new(
                SprigCode::MoveSourceDiverged,
                format!("the source no longer has position {from}"),
            ));
        }
        Ok(Box::new(SimStream {
            db: self.db.clone(),
            notify: self.notify.clone(),
            world: self.world.clone(),
            next: s + 1,
            tables,
            drop: self.rates.stream_drop,
        }))
    }

    async fn write_watermark(&self, wm: &Watermark) -> Result<()> {
        self.call(self.rates.source_err, "watermark").await?;
        lock(&self.db).log(Entry::Wm(*wm));
        self.notify.notify_waiters();
        Ok(())
    }

    async fn read_chunk(
        &self,
        t: &TableMeta,
        after: Option<&Row>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        self.call(self.rates.source_err, "read_chunk").await?;
        let db = lock(&self.db);
        let lo = key_of(after);
        Ok(db
            .src
            .get(&t.name)
            .map(|rows| {
                rows.iter()
                    .filter(|(k, _)| lo.is_none_or(|lo| **k > lo))
                    .take(limit)
                    .map(|(k, v)| row(*k, v))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn digests(
        &self,
        t: &TableMeta,
        after: Option<&Row>,
        limit: usize,
    ) -> Result<Vec<RowDigest>> {
        self.call(self.rates.source_err, "digests").await?;
        let db = lock(&self.db);
        let lo = key_of(after);
        Ok(db
            .src
            .get(&t.name)
            .map(|rows| {
                rows.iter()
                    .filter(|(k, _)| lo.is_none_or(|lo| **k > lo))
                    .take(limit)
                    .map(|(k, v)| digest(t, *k, v))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn read_range(&self, t: &TableMeta, after: Option<&Row>, last: &Row) -> Result<Vec<Row>> {
        self.call(self.rates.source_err, "read_range").await?;
        let db = lock(&self.db);
        let lo = key_of(after);
        let hi = key_of(Some(last)).unwrap_or(i64::MAX);
        Ok(db
            .src
            .get(&t.name)
            .map(|rows| {
                rows.iter()
                    .filter(|(k, _)| lo.is_none_or(|lo| **k > lo) && **k <= hi)
                    .map(|(k, v)| row(*k, v))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn plan_fence(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({ "sim": "planned" }))
    }

    async fn fence(&self, deadline: Duration) -> Result<Fenced> {
        self.call(self.rates.source_err, "fence").await?;
        if self.world.rng().chance(self.rates.fence_fail) {
            self.world.record("fault fence fails");
            return Err(SprigError::new(SprigCode::ClusterFenceFailed, "injected"));
        }
        if self.world.rng().chance(self.rates.fence_slow) {
            self.world.record("fault fence slow");
            tokio::time::sleep(Duration::from_millis(3000)).await;
            if deadline < Duration::from_millis(3000) {
                return Err(SprigError::new(SprigCode::ClusterFenceFailed, "deadline"));
            }
        }
        let mut db = lock(&self.db);
        if db.xa.locked() && self.xa.is_some_and(|m| !m.kill_fence) {
            // The rename waits for the prepared branch's metadata lock
            // until its lock wait ends.
            drop(db);
            xa::fence_refused();
            self.world
                .record("fence refused: a prepared branch holds a lock");
            return Err(SprigError::new(
                SprigCode::ClusterFenceFailed,
                "lock wait: a prepared branch holds the table",
            ));
        }
        db.fenced = true;
        let s = db.log(Entry::Ddl(format!(
            "RENAME TABLE `{SCHEMA}`.`a` TO `{SCHEMA}`.`__sprig_f_0`"
        )));
        self.notify.notify_waiters();
        self.world.record(format!("source fenced at {s}"));
        Ok(Fenced {
            receipt: serde_json::json!({ "sim": s }),
            pos: GtidPos::from_gtids([gtid(s)]),
        })
    }

    async fn fence_reads(&self, _receipt: &serde_json::Value) -> Result<()> {
        Ok(())
    }

    async fn drain_prepared(&self, within: Duration) -> Result<sprig_mover::Drained> {
        match self.xa {
            Some(m) if m.drain => {
                self.call(self.rates.source_err, "drain").await?;
                Ok(xa::drain(&self.db, &self.notify, &self.world, within).await)
            }
            _ => Ok(sprig_mover::Drained::default()),
        }
    }

    async fn unfence(&self, _receipt: &serde_json::Value) -> Result<()> {
        self.call(self.rates.source_err, "unfence").await?;
        self.unfence_lost().await
    }

    async fn unfence_lost(&self) -> Result<()> {
        let mut db = lock(&self.db);
        if db.fenced {
            db.fenced = false;
            db.log(Entry::Ddl(format!(
                "RENAME TABLE `{SCHEMA}`.`__sprig_f_0` TO `{SCHEMA}`.`a`"
            )));
            self.notify.notify_waiters();
            self.world.record("source unfenced");
        }
        Ok(())
    }

    async fn counters(&self) -> Result<Vec<Counter>> {
        Ok(Vec::new())
    }

    async fn load(&self) -> Result<Load> {
        Ok(Load::default())
    }

    async fn drop_schemas(&self) -> Result<()> {
        let mut db = lock(&self.db);
        db.dropped = true;
        db.src.clear();
        Ok(())
    }
}

/// A stream on the source model.
struct SimStream {
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    world: World,
    next: u64,
    tables: Arc<Vec<Arc<TableMeta>>>,
    drop: f64,
}

#[async_trait::async_trait]
impl ChangeStream for SimStream {
    async fn next(&mut self) -> Result<StreamItem> {
        loop {
            let notified = self.notify.notified();
            {
                let db = lock(&self.db);
                if self.next <= db.purged {
                    return Err(SprigError::new(
                        SprigCode::MoveSourceDiverged,
                        "binlog purged past the stream",
                    ));
                }
                if let Some(e) = usize::try_from(self.next - 1)
                    .ok()
                    .and_then(|i| db.binlog.get(i))
                    .cloned()
                {
                    drop(db);
                    if self.world.rng().chance(self.drop) {
                        self.world.record("fault stream drop");
                        return Err(SprigError::new(SprigCode::BinlogDisconnected, "injected"));
                    }
                    let seq = self.next;
                    self.next += 1;
                    let mut txn = SourceTxn {
                        gtid: Some(gtid(seq)),
                        ..SourceTxn::default()
                    };
                    match e {
                        Entry::Rows(changes) => {
                            for (t, b, a) in changes {
                                let Some(meta) = self.tables.iter().find(|m| m.name == t) else {
                                    continue;
                                };
                                txn.changes.push(RowChange {
                                    table: meta.clone(),
                                    before: b.map(|(k, v)| row(k, &v)),
                                    after: a.map(|(k, v)| row(k, &v)),
                                });
                            }
                        }
                        Entry::Wm(w) => txn.watermarks.push(w),
                        Entry::Ddl(sql) => {
                            txn.ddl = Some(SourceDdl {
                                db: SCHEMA.to_owned(),
                                sql,
                            });
                        }
                    }
                    return Ok(StreamItem::Txn(txn));
                }
            }
            tokio::select! {
                biased;
                () = notified => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => return Ok(StreamItem::Idle),
            }
        }
    }
}

/// The target model.
struct SimTarget {
    db: Arc<Mutex<Db>>,
    world: World,
    rates: Rates,
}

impl SimTarget {
    async fn call(&self, p: f64, what: &str) -> Result<()> {
        self.world.chaos_yield().await;
        tokio::time::sleep(Duration::from_micros(200 + self.world.rng().below(800))).await;
        if self.world.rng().chance(p) {
            self.world.record(format!("fault target {what}"));
            return Err(fault(what));
        }
        Ok(())
    }

    fn check_token(db: &Db, token: u64) -> Result<()> {
        match &db.tgt_pos {
            Some(p) if p.token == token => Ok(()),
            _ => Err(SprigError::new(SprigCode::JobLeaseLost, "stale token")),
        }
    }
}

#[async_trait::async_trait]
impl TargetIo for SimTarget {
    async fn prepare(&self, tables: &[TableMeta], _objects: &[String]) -> Result<()> {
        self.call(self.rates.target_err_before, "prepare").await?;
        let mut db = lock(&self.db);
        for t in tables {
            db.tgt.entry(t.name.clone()).or_default();
            db.tgt_binlog.push(TgtEntry::Ddl(format!(
                "CREATE TABLE IF NOT EXISTS `{SCHEMA}`.`{}` (k BIGINT PRIMARY KEY, v TEXT)",
                t.name
            )));
        }
        Ok(())
    }

    async fn load_pos(&self, _job: JobId) -> Result<Option<PosRow>> {
        self.call(self.rates.target_err_before, "load_pos").await?;
        Ok(lock(&self.db).tgt_pos.clone())
    }

    async fn claim(&self, _job: JobId, token: u64, init: &PosRow) -> Result<PosRow> {
        self.call(self.rates.target_err_before, "claim").await?;
        let mut db = lock(&self.db);
        match db.tgt_pos.as_mut() {
            None => {
                let mut row = init.clone();
                row.token = token;
                db.tgt_pos = Some(row.clone());
                Ok(row)
            }
            Some(p) if p.token > token => {
                Err(SprigError::new(SprigCode::JobLeaseLost, "a newer owner"))
            }
            Some(p) => {
                p.token = token;
                Ok(p.clone())
            }
        }
    }

    async fn apply(
        &self,
        _job: JobId,
        token: u64,
        ops: &[ApplyOp],
        pos: &GtidPos,
        copy: &CopyState,
    ) -> Result<()> {
        self.call(self.rates.target_err_before, "apply").await?;
        {
            let mut db = lock(&self.db);
            Self::check_token(&db, token)?;
            let mut logged = Vec::new();
            for op in ops {
                match op {
                    ApplyOp::Upsert { table, rows } => {
                        for r in rows {
                            let Some((k, v)) = parse_row(r) else { continue };
                            logged.push((table.name.clone(), None, Some((k, v.clone()))));
                            if !db.versions.contains(&(table.name.clone(), k, v.clone())) {
                                self.world.fail(format!(
                                    "invariant 2: the target got {}.{k} = {v:?}, a version the \
                                     source never had",
                                    table.name
                                ));
                            }
                            db.tgt.entry(table.name.clone()).or_default().insert(k, v);
                        }
                    }
                    ApplyOp::Delete { table, keys } => {
                        for kv in keys {
                            if let Some(k) = key_of(Some(kv)) {
                                db.tgt.entry(table.name.clone()).or_default().remove(&k);
                                logged.push((table.name.clone(), Some((k, String::new())), None));
                            }
                        }
                    }
                }
            }
            db.tgt_binlog.push(TgtEntry::Apply(logged));
            if let Some(p) = db.tgt_pos.as_mut() {
                p.gtid_pos = pos.clone();
                p.copy = copy.clone();
            }
        }
        if self.world.rng().chance(self.rates.target_err_after) {
            self.world.record("fault target commit reply lost");
            return Err(fault("commit reply lost"));
        }
        Ok(())
    }

    async fn apply_ddl(
        &self,
        job: JobId,
        token: u64,
        _db: &str,
        _sql: &str,
        pos: &GtidPos,
        copy: &CopyState,
    ) -> Result<()> {
        self.apply(job, token, &[], pos, copy).await
    }

    async fn digests(
        &self,
        t: &TableMeta,
        after: Option<&Row>,
        last: &Row,
    ) -> Result<Vec<RowDigest>> {
        self.call(self.rates.target_err_before, "digests").await?;
        let db = lock(&self.db);
        let lo = key_of(after);
        let hi = key_of(Some(last)).unwrap_or(i64::MAX);
        Ok(db
            .tgt
            .get(&t.name)
            .map(|rows| {
                rows.iter()
                    .filter(|(k, _)| lo.is_none_or(|lo| **k > lo) && **k <= hi)
                    .map(|(k, v)| digest(t, *k, v))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn delete_keys(&self, job: JobId, token: u64, t: &TableMeta, keys: &[Row]) -> Result<()> {
        let pos = lock(&self.db)
            .tgt_pos
            .as_ref()
            .map(|p| (p.gtid_pos.clone(), p.copy.clone()));
        let Some((pos, copy)) = pos else {
            return Err(SprigError::new(SprigCode::JobLeaseLost, "no position"));
        };
        let op = ApplyOp::Delete {
            table: Arc::new(t.clone()),
            keys: keys.to_vec(),
        };
        self.apply(job, token, &[op], &pos, &copy).await
    }

    async fn raise_counters(&self, _counters: &[Counter]) -> Result<()> {
        self.call(self.rates.target_err_before, "counters").await
    }

    async fn finalize(&self, _triggers: &[String]) -> Result<GtidPos> {
        self.call(self.rates.target_err_before, "finalize").await?;
        let n = lock(&self.db).tgt_binlog.len() as u64;
        Ok(if n == 0 {
            GtidPos::new()
        } else {
            GtidPos::from_gtids([feed::tgt_gtid(n)])
        })
    }

    async fn unfinalize(&self, _triggers: &[String]) -> Result<()> {
        Ok(())
    }

    async fn reset(&self, _job: JobId, _keep_schemas: bool) -> Result<()> {
        self.call(self.rates.target_err_before, "reset").await?;
        let mut db = lock(&self.db);
        for rows in db.tgt.values_mut() {
            rows.clear();
        }
        db.tgt_pos = None;
        db.tgt_binlog.push(TgtEntry::Apply(Vec::new()));
        self.world.record("target reset");
        Ok(())
    }

    async fn forget(&self, _job: JobId) -> Result<()> {
        let mut db = lock(&self.db);
        db.tgt_pos = None;
        db.tgt_binlog.push(TgtEntry::Apply(Vec::new()));
        Ok(())
    }

    async fn load(&self) -> Result<Load> {
        Ok(Load::default())
    }
}

/// The production map commit, with invariants 4 to 6 checked around it.
struct CheckedMaps {
    inner: TenantMapIo,
    db: Arc<Mutex<Db>>,
    world: World,
    clock: Arc<SimClock>,
    held_at: Mutex<Option<Instant>>,
    budget: Duration,
}

#[async_trait::async_trait]
impl MapIo for CheckedMaps {
    async fn hold(&self, job: JobId, token: u64, until_ms: u64) -> Result<u64> {
        let e = self.inner.hold(job, token, until_ms).await?;
        *self.held_at.lock().unwrap_or_else(PoisonError::into_inner) = Some(self.clock.now());
        self.world.record("map held");
        Ok(e)
    }

    async fn release(&self, job: JobId, token: u64) -> Result<Released> {
        let r = self.inner.release(job, token).await?;
        self.world.record(format!("map release: {r:?}"));
        Ok(r)
    }

    async fn commit(&self, job: JobId, token: u64, to: &ClusterId) -> Result<CommitOutcome> {
        self.before_commit();
        let o = self.inner.commit(job, token, to).await?;
        self.after_commit(o);
        Ok(o)
    }

    async fn commit_handoff(
        &self,
        job: JobId,
        token: u64,
        to: &ClusterId,
        handoff: &HandoffPos,
    ) -> Result<CommitOutcome> {
        self.before_commit();
        let o = self.inner.commit_handoff(job, token, to, handoff).await?;
        self.after_commit(o);
        Ok(o)
    }

    async fn bump(&self, job: JobId, token: u64) -> Result<CommitOutcome> {
        self.inner.bump(job, token).await
    }

    async fn read(&self) -> Result<MapState> {
        self.inner.read().await
    }
}

impl CheckedMaps {
    /// Invariants 3 and 4, just before the commit point.
    fn before_commit(&self) {
        {
            let db = lock(&self.db);
            if !db.fenced {
                self.world
                    .fail("invariant 3: commit while the source is unfenced");
            }
            let empty = BTreeMap::new();
            for (t, rows) in &db.src {
                if db.tgt.get(t).unwrap_or(&empty) != rows {
                    self.world.fail(format!(
                        "invariant 4: at commit, table {t} differs: source {} rows, target {} rows",
                        rows.len(),
                        db.tgt.get(t).map_or(0, BTreeMap::len)
                    ));
                }
            }
        }
    }

    /// Invariant 6, once committed.
    fn after_commit(&self, o: CommitOutcome) {
        if o == CommitOutcome::Committed {
            let held = self
                .held_at
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .map(|t| self.clock.now().saturating_duration_since(t));
            if let Some(h) = held
                && h > self.budget
            {
                self.world.fail(format!(
                    "invariant 6: the hold lasted {h:?}, over {:?}",
                    self.budget
                ));
            }
            self.world.record(format!("map committed after {held:?}"));
        }
    }
}

/// Builds each claimed job's environment on the models.
struct Factory {
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    world: World,
    node: Node,
    rates: Rates,
    tables: Vec<String>,
    settings: MoverSettings,
    xa: Option<xa::XaMode>,
}

#[async_trait::async_trait]
impl EnvFactory for Factory {
    async fn env(&self, _rec: &JobRecord) -> Result<JobEnv> {
        let tier = Tier::Dc(
            sprig_common::ids::DcId::new(DC).map_err(|e| SprigError::internal(e.to_string()))?,
        );
        let meta = self.world.etcd().client(&self.node, tier);
        Ok(JobEnv {
            source: Arc::new(SimSource {
                db: self.db.clone(),
                notify: self.notify.clone(),
                world: self.world.clone(),
                rates: self.rates,
                tables: self.tables.clone(),
                xa: self.xa,
            }),
            target: Arc::new(SimTarget {
                db: self.db.clone(),
                world: self.world.clone(),
                rates: self.rates,
            }),
            maps: Arc::new(CheckedMaps {
                inner: TenantMapIo::new(TenantMaps::new(meta), TENANT),
                db: self.db.clone(),
                world: self.world.clone(),
                clock: self.world.clock(&self.node),
                held_at: Mutex::new(None),
                budget: self.settings.cutover_max_hold + self.settings.hold_grace,
            }),
        })
    }

    fn settings(&self, _rec: &JobRecord) -> MoverSettings {
        self.settings.clone()
    }
}

fn cluster(n: &str) -> ClusterId {
    ClusterId::new(n).unwrap_or_else(|_| unreachable!("valid cluster id"))
}

/// One client write, applied to the model under its lock.
struct ClientWrite<'a> {
    to_source: bool,
    table: &'a str,
    k: i64,
    /// `None` deletes.
    v: Option<&'a str>,
    ddl: bool,
    at: Duration,
}

impl ClientWrite<'_> {
    /// Applies the write; `false` if the source refused it (fenced).
    fn apply(&self, db: &Mutex<Db>) -> bool {
        let mut d = lock(db);
        let (t, k) = (self.table.to_owned(), self.k);
        if self.to_source {
            if d.fenced || d.dropped {
                return false;
            }
            if self.ddl {
                // DDL acknowledges nothing about rows.
                d.ddl_seq += 1;
                let s = d.ddl_seq;
                d.log(Entry::Ddl(format!(
                    "ALTER TABLE `{SCHEMA}`.`{t}` COMMENT 'c{s}'"
                )));
                return true;
            }
            let before = d.src.get(&t).and_then(|r| r.get(&k)).cloned();
            let after = self.v.map(|v| (k, v.to_owned()));
            match &after {
                None => {
                    if let Some(r) = d.src.get_mut(&t) {
                        r.remove(&k);
                    }
                }
                Some((_, v)) => {
                    d.src.entry(t.clone()).or_default().insert(k, v.clone());
                    d.versions.insert((t.clone(), k, v.clone()));
                }
            }
            if before.is_some() || after.is_some() {
                d.log(Entry::Rows(vec![(
                    t.clone(),
                    before.map(|b| (k, b)),
                    after,
                )]));
            }
            d.last_src_ack = Some(self.at);
        } else {
            if self.ddl {
                return true;
            }
            let r = d.tgt.entry(t.clone()).or_default();
            let before = r.get(&k).cloned().map(|b| (k, b));
            match self.v {
                None => {
                    r.remove(&k);
                }
                Some(v) => {
                    r.insert(k, v.to_owned());
                }
            }
            let after = self.v.map(|v| (k, v.to_owned()));
            if before.is_some() || after.is_some() {
                d.tgt_binlog
                    .push(TgtEntry::Client((t.clone(), before, after)));
            }
            d.first_tgt_ack.get_or_insert(self.at);
        }
        d.acked.insert((t, k), self.v.map(str::to_owned));
        true
    }
}

/// One client: routes by the map like a gateway, writes its own keys.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn client(
    world: World,
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    maps: TenantMaps,
    me: usize,
    of: usize,
    tables: Vec<String>,
    rows: i64,
    stop: CancellationToken,
    src: ClusterId,
) {
    let node = Node::new(format!("gw{me}"));
    let clock = world.clock(&node);
    let late = world.rng().chance(0.3);
    let mut cached: Option<(Placement, Instant)> = None;
    let mut n = 0u64;
    let keys: Vec<i64> = (0..rows * 2 + 4)
        .filter(|k| usize::try_from(*k).unwrap_or(0) % of == me)
        .collect();
    if keys.is_empty() {
        return;
    }
    while !stop.is_cancelled() {
        tokio::time::sleep(
            world
                .rng()
                .duration(Duration::from_millis(20), Duration::from_millis(120)),
        )
        .await;
        n += 1;
        let t = world.rng().pick(&tables).clone();
        let k = *world.rng().pick(&keys);
        let del = world.rng().chance(0.2);
        let ddl = world.rng().chance(0.01);
        let v = format!("c{me}-{n}");
        let started = clock.now();
        loop {
            if stop.is_cancelled() {
                return;
            }
            let stale = cached.as_ref().is_some_and(|(_, at)| {
                clock.now().saturating_duration_since(*at) > Duration::from_millis(200)
            });
            if cached.is_none() || stale {
                let Ok((m, _)) = maps.load(TENANT).await else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                cached = Placement::of(&m).ok().map(|p| (p, clock.now()));
            }
            let Some((p, _)) = cached.clone() else {
                continue;
            };
            let now_ms = sprig_mover::record::wall_ms(clock.wall());
            if p.held_at(now_ms) && !late {
                tokio::time::sleep(Duration::from_millis(10)).await;
                cached = None;
                continue;
            }
            let elapsed = world.elapsed();
            let w = ClientWrite {
                to_source: p.cluster == src,
                table: &t,
                k,
                v: (!del).then_some(v.as_str()),
                ddl,
                at: elapsed,
            };
            if !w.apply(&db) {
                // A fence error: reload the map and retry.
                cached = None;
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            notify.notify_waiters();
            let waited = clock.now().saturating_duration_since(started);
            if waited > Duration::from_millis(500) {
                world.record(format!("client {me} waited {waited:?}"));
            }
            break;
        }
    }
}

/// Runs one seed: a tenant move under load with faults, then the checks.
///
/// # Errors
/// The first broken invariant.
pub async fn run_move(world: World) -> std::result::Result<(), String> {
    run_move_with(world, Variant::default()).await
}

/// Runs one seed of an idle tenant (no client writes): another job holds
/// the target's copy slots for a while in some seeds, and in most seeds an
/// operator cancels the job in a state drawn from every state before the
/// commit. A cancel accepted before `ready` must end
/// `rolled_back`; every job must end (invariant 7).
///
/// # Errors
/// The first broken invariant.
pub async fn run_idle_move(world: World) -> std::result::Result<(), String> {
    run_move_with(
        world,
        Variant {
            idle: true,
            cancel: true,
            ..Variant::default()
        },
    )
    .await
}

/// What a run changes from the default scenario.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_excessive_bools)] // independent switches of one scenario
pub struct Variant {
    /// No clients: nothing writes to the tenant during its move.
    pub idle: bool,
    /// Hog the target's copy slots for a while, and cancel the job in a
    /// drawn state.
    pub cancel: bool,
    /// Run an after-commit trigger's worker on the tenant's feed too,
    /// killed at random (invariant 9).
    pub triggers: bool,
    /// A coordinator prepares distributed-transaction branches on the
    /// source ([`xa`]); the cutover drains them (cutover steps 2b and
    /// 3b) unless `xa_no_drain`.
    pub xa: bool,
    /// The XA variant without the drain: the behaviour before it.
    pub xa_no_drain: bool,
    /// Run a change topic's sequencer on the tenant's feed too, killed
    /// and overlapped at random (the stream invariant).
    pub streams: bool,
    /// The sequencer (and the trigger) reads through the shared
    /// per-cluster reader (`sprig_feed::shared`) with tiny buffers and
    /// ring, so
    /// its subscription is detached, comes back through the ring, and
    /// falls back to catch-up dumps (the same stream invariant).
    pub shared_feed: bool,
}

/// Runs one seed of [`run_move`] with an after-commit trigger on the
/// tenant's feed, whose worker is killed at random: every client change
/// must run exactly once (invariant 9).
///
/// # Errors
/// The first broken invariant.
pub async fn run_move_triggers(world: World) -> std::result::Result<(), String> {
    run_move_with(
        world,
        Variant {
            triggers: true,
            ..Variant::default()
        },
    )
    .await
}

/// [`run_move_triggers`] with the trigger's feed reading through the
/// shared per-cluster reader (R9, r9feed2), with tiny buffers and ring:
/// its subscription is detached, comes back through the ring, a replay or
/// a catch-up dump, and follows the move. Every change must still run
/// exactly once.
///
/// # Errors
/// The first broken invariant.
pub async fn run_move_triggers_shared(world: World) -> std::result::Result<(), String> {
    run_move_with(
        world,
        Variant {
            triggers: true,
            shared_feed: true,
            ..Variant::default()
        },
    )
    .await
}

/// Runs one seed of [`run_move`] with a change topic's sequencer on the
/// tenant's feed, killed, overlapped by a second sequencer
/// (a lost lease) and failing its appends at random: every change of the
/// topic's table is in the log once, in commit order per partition.
///
/// # Errors
/// The first broken invariant.
pub async fn run_move_streams(world: World) -> std::result::Result<(), String> {
    run_move_with(
        world,
        Variant {
            streams: true,
            ..Variant::default()
        },
    )
    .await
}

/// [`run_move_streams`] with the sequencer reading through the shared
/// per-cluster reader: its tail subscribes to one hub per
/// cluster, whose tiny buffers and ring make it detach, return through the
/// ring and fall back to catch-up dumps while the tenant moves, streams
/// drop and the sequencer is killed. The stream invariant must hold.
///
/// # Errors
/// The first broken invariant.
pub async fn run_move_streams_shared(world: World) -> std::result::Result<(), String> {
    run_move_with(
        world,
        Variant {
            streams: true,
            shared_feed: true,
            ..Variant::default()
        },
    )
    .await
}

/// Runs one seed of [`run_move`] with a coordinator preparing
/// distributed-transaction branches on the source ([`xa`]): crashed and
/// stalled coordinators leave branches prepared at cutover, which the
/// cutover drains (cutover steps 2b and 3b). Nothing may be lost: a
/// branch committed after `G` would be missing on the target.
///
/// # Errors
/// The first broken invariant.
pub async fn run_move_xa(world: World) -> std::result::Result<(), String> {
    run_move_with(
        world,
        Variant {
            xa: true,
            ..Variant::default()
        },
    )
    .await
}

/// [`run_move_xa`] without the drain: the cutover's behaviour before it.
/// Under a kill fence, a branch committed after `G` is lost, which the
/// checks find.
///
/// # Errors
/// The first broken invariant.
pub async fn run_move_xa_no_drain(world: World) -> std::result::Result<(), String> {
    run_move_with(
        world,
        Variant {
            xa: true,
            xa_no_drain: true,
            ..Variant::default()
        },
    )
    .await
}

/// The copy slots another job holds on the target, and when they free.
const HOG: &str = "sim-hog";

#[allow(clippy::too_many_lines)]
async fn run_move_with(world: World, variant: Variant) -> std::result::Result<(), String> {
    let rng = world.rng().fork();
    let mut sc = MoverScenario::random(&rng);
    let rates = Rates::draw(&rng, sc.faults);
    if variant.idle {
        sc.clients = 0;
    }
    let xa_mode = variant.xa.then(|| xa::XaMode {
        kill_fence: rng.chance(0.5),
        drain: !variant.xa_no_drain,
    });
    world.record(format!(
        "scenario {sc:?} rates {rates:?} variant {variant:?}"
    ));
    let dc = sprig_common::ids::DcId::new(DC).map_err(|e| e.to_string())?;
    let tier = Tier::Dc(dc);
    let (src, tgt) = (cluster("dc-a-c1"), cluster("dc-a-c2"));
    let tables: Vec<String> = ["a", "b", "c"][..sc.tables]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    let db = Arc::new(Mutex::new(Db::default()));
    let notify = Arc::new(Notify::new());
    {
        let mut d = lock(&db);
        for t in &tables {
            let mut changes = Vec::new();
            for k in 0..sc.rows {
                let v = format!("init-{k}");
                d.src.entry(t.clone()).or_default().insert(k, v.clone());
                d.versions.insert((t.clone(), k, v.clone()));
                d.acked.insert((t.clone(), k), Some(v.clone()));
                changes.push((t.clone(), None, Some((k, v))));
            }
            d.src.entry(t.clone()).or_default();
            d.log(Entry::Rows(changes));
        }
    }
    // The tenant's map, on the source.
    let admin = world.etcd().client(&Node::from("admin"), tier.clone());
    let key = ShardMap::catalog_key(&tier, TENANT).map_err(|e| e.to_string())?;
    admin
        .txn(Txn {
            when: vec![Cmp::Missing(key.clone())],
            then: vec![Op::Put {
                key,
                value: Record::new(ShardMap::catalog(TENANT, src.clone(), 1))
                    .encode()
                    .map_err(|e| e.to_string())?,
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .map_err(|e| e.to_string())?;
    let mut settings = MoverSettings::for_tests();
    settings.job_lease = Duration::from_secs(3);
    let admin_clock: Arc<dyn Clock> = world.clock(&Node::from("admin"));
    let mover = MetaMover::new(JobStore::new(admin.clone(), admin_clock));
    let id = mover
        .submit(JobSpec::move_tenant(TENANT, src.clone(), tgt.clone()))
        .await
        .map_err(|e| e.to_string())?;

    // Two workers; either may claim; crashed ones restart.
    let stop = CancellationToken::new();
    let workers: Vec<Node> = vec![Node::from("w1"), Node::from("w2")];
    let start_worker = |n: &Node| {
        let factory = Arc::new(Factory {
            db: db.clone(),
            notify: notify.clone(),
            world: world.clone(),
            node: n.clone(),
            rates,
            tables: tables.clone(),
            settings: settings.clone(),
            xa: xa_mode,
        });
        let clock: Arc<dyn Clock> = world.clock(n);
        let store = JobStore::new(world.etcd().client(n, tier.clone()), clock);
        let mut opts = WorkerOpts::new(&n.0);
        opts.lease = settings.job_lease;
        opts.scan_interval = Duration::from_millis(500);
        let rt = JobRuntime::new(store, factory, opts);
        let tok = stop.child_token();
        let t = tok.clone();
        (tokio::spawn(async move { rt.run(t).await }), tok)
    };
    let mut running: BTreeMap<String, (tokio::task::JoinHandle<Result<()>>, CancellationToken)> =
        BTreeMap::new();
    for w in &workers {
        running.insert(w.0.clone(), start_worker(w));
    }

    // Clients.
    let mut clients = Vec::new();
    for i in 0..sc.clients {
        let node = Node::new(format!("gw{i}"));
        let maps = TenantMaps::new(world.etcd().client(&node, tier.clone()));
        clients.push(tokio::spawn(client(
            world.clone(),
            db.clone(),
            notify.clone(),
            maps,
            i,
            sc.clients,
            tables.clone(),
            sc.rows,
            stop.clone(),
            src.clone(),
        )));
    }

    // Distributed transactions with branches on the source, and the
    // controller's resolver.
    if xa_mode.is_some() {
        let node = Node::from("coord");
        let maps = TenantMaps::new(world.etcd().client(&node, tier.clone()));
        clients.push(tokio::spawn(xa::coordinator(
            world.clone(),
            db.clone(),
            notify.clone(),
            maps,
            tables.clone(),
            stop.clone(),
            src.clone(),
        )));
        clients.push(tokio::spawn(xa::resolver(
            world.clone(),
            db.clone(),
            notify.clone(),
            stop.clone(),
        )));
    }

    // A feed consumer reads the tenant's feed across the move.
    let feed_clusters = Arc::new(feed::SimClusters {
        db: db.clone(),
        notify: notify.clone(),
        world: world.clone(),
        drop: rates.stream_drop,
        src: src.clone(),
        tgt: tgt.clone(),
    });
    let feed_goal: feed::Goal = Arc::default();
    let feed_log: Arc<Mutex<feed::FeedLog>> = Arc::default();
    let feed_stop = CancellationToken::new();
    let trig_log: Arc<Mutex<logic::TriggerLog>> = Arc::default();
    let mut trig_task = variant.triggers.then(|| {
        tokio::spawn(logic::trigger_worker(
            world.clone(),
            feed_clusters.clone(),
            variant.shared_feed,
            tier.clone(),
            sc.faults,
            feed_goal.clone(),
            trig_log.clone(),
            feed_stop.clone(),
        ))
    });
    let stream_log = streams::new_log(&world, &feed_clusters, sc.faults);
    let stream_run: Arc<Mutex<streams::StreamRun>> = Arc::default();
    let mut stream_task = variant.streams.then(|| {
        tokio::spawn(streams::sequencer_worker(
            world.clone(),
            feed_clusters.clone(),
            variant.shared_feed,
            tier.clone(),
            sc.faults,
            feed_goal.clone(),
            stream_log.clone(),
            stream_run.clone(),
            feed_stop.clone(),
        ))
    });
    let mut feed_task = tokio::spawn(feed::consumer(
        world.clone(),
        feed_clusters,
        tier.clone(),
        sc.faults,
        feed_goal.clone(),
        feed_log.clone(),
        feed_stop.clone(),
    ));

    // Faults: crash the owning worker at a chosen state, partition a
    // worker from etcd, purge the binlog.
    let crash_states = [
        JobState::Preparing,
        JobState::Copying,
        JobState::CatchingUp,
        JobState::Verifying,
        JobState::Ready,
        JobState::CuttingOver,
        JobState::Retaining,
    ];
    let crash_at = sc.faults.then(|| *rng.pick(&crash_states));
    let mut crashed = false;
    let partition_at = (sc.faults && rng.chance(0.3))
        .then(|| rng.duration(Duration::from_secs(1), Duration::from_secs(20)));
    let mut partitioned = None;
    let purge_at = (sc.faults && rng.chance(0.2))
        .then(|| rng.duration(Duration::from_millis(200), Duration::from_secs(10)));
    let deadline = Duration::from_secs(1200);
    // Drawn after every default draw, so the default scenario's seeds
    // replay as before.
    let cancel_states = [
        JobState::Planned,
        JobState::Preparing,
        JobState::Copying,
        JobState::CatchingUp,
        JobState::Verifying,
        JobState::Ready,
    ];
    let cancel_at = (variant.cancel && rng.chance(0.8)).then(|| *rng.pick(&cancel_states));
    let mut cancelled: Option<JobState> = None;
    let mut cancelled_at = Duration::ZERO;
    let hog_until = (variant.cancel && rng.chance(0.4))
        .then(|| rng.duration(Duration::from_secs(1), Duration::from_secs(300)));
    if let Some(until) = hog_until {
        let n = settings.max_copies_per_cluster.max(1);
        let mut then = Vec::new();
        for i in 0..n {
            then.push(Op::Put {
                key: tier
                    .key(&format!("mover/slots/copy/{tgt}/{i}"))
                    .map_err(|e| e.to_string())?,
                value: HOG.as_bytes().to_vec(),
                lease: None,
            });
        }
        admin
            .txn(Txn {
                when: vec![],
                then,
                otherwise: vec![],
            })
            .await
            .map_err(|e| e.to_string())?;
        world.record(format!(
            "another job holds {tgt}'s copy slots for {until:?}"
        ));
    }
    let mut hogging = hog_until.is_some();
    let started = world.elapsed();
    let end_state = loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = world.elapsed().saturating_sub(started);
        if now > deadline {
            return Err(format!(
                "invariant 7: the job didn't end within {deadline:?}"
            ));
        }
        if hogging && hog_until.is_some_and(|u| now >= u) {
            hogging = false;
            let n = settings.max_copies_per_cluster.max(1);
            let mut then = Vec::new();
            for i in 0..n {
                then.push(Op::Delete(
                    tier.key(&format!("mover/slots/copy/{tgt}/{i}"))
                        .map_err(|e| e.to_string())?,
                ));
            }
            let _ = admin
                .txn(Txn {
                    when: vec![],
                    then,
                    otherwise: vec![],
                })
                .await;
            world.record("the other job freed the copy slots");
        }
        let Ok(st) = mover.status(&id).await else {
            continue;
        };
        if st.state.is_terminal() || (st.state == JobState::Paused && cancelled.is_none()) {
            break st;
        }
        // A cancel takes effect promptly in every state: within the
        // lease and a few polls, not when whatever it waits for ends.
        if cancelled.is_some_and(|c| c != JobState::Ready)
            && now.saturating_sub(cancelled_at) > Duration::from_secs(60)
        {
            return Err(format!(
                "cancel: accepted in {:?} {:?} ago, the job is still {}",
                cancelled,
                now.saturating_sub(cancelled_at),
                st.state
            ));
        }
        if let Some(cs) = cancel_at
            && cancelled.is_none()
            && st.state == cs
            && rng.chance(0.3)
        {
            match mover.cancel(&id, "sim").await {
                Ok(()) => {
                    world.record(format!("cancel accepted in {}", st.state));
                    cancelled = Some(st.state);
                    cancelled_at = now;
                }
                Err(e) => world.record(format!("cancel refused in {}: {e}", st.state)),
            }
        }
        if let Some(cs) = crash_at
            && !crashed
            && st.state == cs
            && rng.chance(0.2)
        {
            crashed = true;
            let owner = mover.store().get(&id).await.ok().and_then(|(r, _)| r.owner);
            if let Some(o) = owner
                && let Some((h, tok)) = running.remove(&o)
            {
                world.record(format!("crash worker {o} in {}", st.state));
                tok.cancel();
                h.abort();
                let n = Node::from(o.as_str());
                let restart = rng.duration(Duration::from_millis(100), Duration::from_secs(5));
                tokio::time::sleep(restart).await;
                running.insert(o.clone(), start_worker(&n));
            }
        }
        if let Some(at) = partition_at
            && partitioned.is_none()
            && now >= at
        {
            let w = rng.pick(&workers).clone();
            world.record(format!("partition {w} from etcd"));
            let f = world.net().partition(&[w], &[world.etcd().node().clone()]);
            partitioned = Some((
                f,
                now + rng.duration(Duration::from_secs(1), Duration::from_secs(15)),
            ));
        }
        if let Some((f, until)) = partitioned
            && now >= until
        {
            world.net().heal(f);
            world.record("partition healed");
            partitioned = Some((f, Duration::MAX));
        }
        if let Some(at) = purge_at
            && now >= at
        {
            let mut d = lock(&db);
            if d.purged == 0 && !st.state.is_committed() {
                d.purged = d.seq();
                world.record(format!("binlog purged to {}", d.purged));
            }
        }
    };
    stop.cancel();
    for c in clients {
        let _ = c.await;
    }
    for (_, (h, _)) in running {
        h.abort();
    }
    if let Some((f, _)) = partitioned {
        world.net().heal(f);
    }
    // The feed consumer catches up with the final owner's binlog.
    let goal = {
        let d = lock(&db);
        match end_state.state {
            JobState::Done => Some((tgt.clone(), d.tgt_binlog.len() as u64)),
            JobState::RolledBack => Some((src.clone(), d.binlog.len() as u64)),
            _ => None,
        }
    };
    let feed_done = match goal {
        Some(g) => {
            *feed_goal.lock().unwrap_or_else(PoisonError::into_inner) = Some(g);
            notify.notify_waiters();
            tokio::time::timeout(Duration::from_secs(600), &mut feed_task)
                .await
                .is_ok()
        }
        None => false,
    };
    let trig_done = match (&mut trig_task, feed_done) {
        (Some(t), true) => tokio::time::timeout(Duration::from_secs(600), t)
            .await
            .is_ok(),
        _ => false,
    };
    let stream_done = match (&mut stream_task, feed_done) {
        (Some(t), true) => tokio::time::timeout(Duration::from_secs(600), t)
            .await
            .is_ok(),
        _ => false,
    };
    feed_stop.cancel();
    feed_task.abort();
    if let Some(t) = &trig_task {
        t.abort();
    }
    if let Some(t) = &stream_task {
        t.abort();
    }
    let history = sprig_feed::ownership::history(&admin, TENANT, 0)
        .await
        .map_err(|e| e.to_string())?;
    if variant.triggers {
        let seen = feed_log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .seen
            .clone();
        logic::check_triggers(&seen, &trig_log, trig_done).await?;
        world.record(logic::summary(&trig_log));
    }
    if variant.streams {
        let seen = feed_log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .seen
            .clone();
        world.record(streams::summary(&stream_log, &stream_run));
        streams::check_stream(&seen, &stream_log, stream_done)?;
    }
    let maps = TenantMaps::new(admin);
    let (m, _) = maps.load(TENANT).await.map_err(|e| e.to_string())?;
    let p = Placement::of(&m).map_err(|e| e.to_string())?;
    let d = lock(&db);
    world.record(format!(
        "end {} epoch {} src_ack {:?} tgt_ack {:?}",
        end_state.state, p.epoch, d.last_src_ack, d.first_tgt_ack
    ));
    if let Some(at) = cancelled
        && at != JobState::Ready
        && end_state.state != JobState::RolledBack
    {
        return Err(format!(
            "cancel: accepted in {at}, but the job ended {}",
            end_state.state
        ));
    }
    let (owner, expect_epoch) = match end_state.state {
        JobState::Done => (&d.tgt, 2),
        JobState::RolledBack => (&d.src, 1),
        s => {
            return Err(format!(
                "invariant 7: the job ended {s}: {:?}",
                end_state.error
            ));
        }
    };
    if p.epoch != expect_epoch || p.hold.is_some() {
        return Err(format!(
            "invariant 5: the map ended at epoch {} (held: {:?}), expected {expect_epoch}",
            p.epoch, p.hold
        ));
    }
    if end_state.state == JobState::Done && p.cluster != tgt {
        return Err("the map doesn't point at the target".into());
    }
    if end_state.state == JobState::Done && d.xa.left() > 0 {
        return Err(format!(
            "invariant 1 (xa): {} prepared branches outlived the move on the fenced source",
            d.xa.left()
        ));
    }
    if let (Some(s), Some(t)) = (d.last_src_ack, d.first_tgt_ack)
        && s > t
    {
        return Err(format!(
            "invariant 3: the source acknowledged a write at {s:?}, after the target did at {t:?}"
        ));
    }
    for ((t, k), v) in &d.acked {
        let got = owner.get(t).and_then(|r| r.get(k));
        if got != v.as_ref() {
            return Err(format!(
                "invariant 1: {t}.{k} is {got:?} on the owner, the last acknowledged write was {v:?}"
            ));
        }
    }
    for (t, rows) in owner {
        for (k, v) in rows {
            if d.acked.get(&(t.clone(), *k)) != Some(&Some(v.clone())) {
                return Err(format!(
                    "invariant 1: {t}.{k} = {v:?} was never acknowledged last"
                ));
            }
        }
    }
    check_feed(
        &d,
        end_state.state,
        &history,
        feed_done,
        &feed_log.lock().unwrap_or_else(PoisonError::into_inner),
    )?;
    let failures = world.failures();
    if let Some(f) = failures.first() {
        return Err(f.clone());
    }
    Ok(())
}

/// Invariant 8: the consumer's durable events are exactly the tenant's
/// committed client changes, in order.
fn check_feed(
    d: &Db,
    end: JobState,
    history: &[sprig_feed::Handoff],
    done: bool,
    log: &feed::FeedLog,
) -> std::result::Result<(), String> {
    if !done {
        return Err(format!(
            "invariant 8: the feed didn't catch up (error {:?}, token {:?})",
            log.error, log.token
        ));
    }
    match (end, history) {
        (JobState::Done, [h]) => {
            let fence = h.fence().map_err(|e| e.to_string())?;
            let start = h.start().map_err(|e| e.to_string())?;
            let f = fence.get(DOMAIN).map_or(0, |g| g.seq);
            let s = start.get(feed::TGT_DOMAIN).map_or(0, |g| g.seq);
            // Nothing of the tenant's after G on the source, nothing of a
            // client's up to H on the target.
            if d.binlog
                .iter()
                .skip(usize::try_from(f).unwrap_or(0))
                .any(|e| {
                    matches!(e, Entry::Rows(_))
                        || matches!(e, Entry::Ddl(sql) if !sql.contains("__sprig_"))
                })
            {
                return Err(format!("invariant 8: the source changed after G = {f}"));
            }
            if d.tgt_binlog
                .iter()
                .take(usize::try_from(s).unwrap_or(0))
                .any(|e| matches!(e, TgtEntry::Client(_)))
            {
                return Err(format!(
                    "invariant 8: a client wrote the target before H = {s}"
                ));
            }
        }
        (JobState::RolledBack, []) => {}
        (e, h) => {
            return Err(format!(
                "invariant 8: the job ended {e} with {} handoff records",
                h.len()
            ));
        }
    }
    let mut expected = Vec::new();
    for e in &d.binlog {
        match e {
            Entry::Rows(c) => {
                for (t, b, a) in c {
                    expected.push(feed::row_text(t, b.as_ref(), a.as_ref()));
                }
            }
            Entry::Ddl(sql) if !sql.contains("__sprig_") => expected.push(feed::ddl_text(sql)),
            _ => {}
        }
    }
    if end == JobState::Done {
        for e in &d.tgt_binlog {
            if let TgtEntry::Client((t, b, a)) = e {
                expected.push(feed::row_text(t, b.as_ref(), a.as_ref()));
            }
        }
    }
    let got: Vec<&str> = log.seen.iter().map(|s| s.what.as_str()).collect();
    if let Some(i) = (0..expected.len().max(got.len()))
        .find(|i| expected.get(*i).map(String::as_str) != got.get(*i).copied())
    {
        return Err(format!(
            "invariant 8: event {i} of the feed is {:?}, expected {:?} ({} expected, {} \
             received, {} consumer restarts)",
            got.get(i),
            expected.get(i),
            expected.len(),
            got.len(),
            log.restarts
        ));
    }
    let mut ids = BTreeSet::new();
    for (i, s) in log.seen.iter().enumerate() {
        if !ids.insert(s.id.clone()) {
            return Err(format!("invariant 8: event id {:?} delivered twice", s.id));
        }
        if i > 0 {
            let p = &log.seen[i - 1];
            let same_txn = (p.id.0.as_str(), p.id.1) == (s.id.0.as_str(), s.id.1);
            if (same_txn && p.clamp != s.clamp) || (!same_txn && p.clamp >= s.clamp) {
                return Err(format!(
                    "invariant 8: clamps out of order: {p:?} then {s:?}"
                ));
            }
        }
    }
    Ok(())
}
