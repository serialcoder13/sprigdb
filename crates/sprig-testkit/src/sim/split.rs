//! Split and merge under deterministic simulation, with a controller crash
//! at every job step.
//!
//! The production pieces run: the job store and claims ([`MetaMover`],
//! [`JobRuntime`]) on the simulated etcd, the engine, the hash-range
//! routing ([`ShardRoute`]) and the group map commit ([`GroupMapIo`]) on
//! a table group's shard map. MariaDB is a model of one cluster: shard
//! schemas of tables `(k BIGINT PRIMARY KEY, v VARCHAR, _sprig_rk)`, one
//! GTID binlog of row images tagged by schema (the mover's own writes into
//! the new shards included, so the stream's schema filter is exercised),
//! watermarks, a rename fence per schema, position rows per job with
//! fencing tokens, and purging.
//!
//! A run splits shard 1 of group 3 at a random point into shards 2 and 3,
//! then merges them back into shard 4. Clients route each write by the
//! group's map like gateways (they honour holds, some late, some with a
//! stale map, and retry after a fence error or a dropped schema), and
//! sometimes change a row's routing hash inside its shard, which moves the
//! row between the children of a running split. Faults: workers crashed in
//! every job state, etcd partitions, dropped streams, failing and
//! reply-lost target commits, failing and slow fences, purged binlogs.
//!
//! Invariants:
//!
//! 1. No acknowledged write is lost or duplicated: every key's last
//!    acknowledged row is in the shard its hash routes to in the final
//!    map, and in no other serving shard; no serving shard holds a row
//!    nobody acknowledged last.
//! 2. A target never holds a row version the source never had.
//! 3. The commit happens only with every source fenced.
//! 4. At commit, each target equals the sources' rows of its range, and
//!    holds no row outside it.
//! 5. The final map is valid, holds nothing, and its epochs rose by one
//!    per committed job.
//! 6. A committed cutover's hold ends within `mover.cutover_max_hold_ms`
//!    plus the hold grace.
//! 7. Every job ends `done` or `rolled_back`; a done job's sources are
//!    dropped, a rolled-back job's targets are.

#![allow(clippy::many_single_char_names)] // keys, values, tables: k, v, t

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_binlog::{Gtid, GtidPos};
use sprig_common::ids::{ClusterId, DbId, GroupId, JobId, ShardId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_meta::Tier;
use sprig_mover::api::HashRange;
use sprig_mover::io::{
    ChangeStream, CommitOutcome, Counter, Fenced, HandoffPos, JobEnv, Load, MapIo, MapState,
    Released, SourceIo, TargetIo,
};
use sprig_mover::model::{
    ApplyOp, ColumnMeta, CopyState, PosRow, Row, RowChange, RowDigest, SourceDdl, SourceTxn,
    StreamItem, TableMeta, Value, Watermark,
};
use sprig_mover::{
    EnvFactory, GroupMapIo, JobRecord, JobRuntime, JobSpec, JobState, JobStore, MetaMover, Mover,
    MoverSettings, ShardRoute, WorkerOpts,
};
use sprig_shard::shardmap::RangeState;
use sprig_shard::{ShardMap, ShardMapStore, shard_schema};
use tokio::sync::Notify;

use super::{SimClock, SimRng, World};
use crate::faults::Node;

const TENANT: TenantId = TenantId(1);
const DB: DbId = DbId(1);
const GROUP: GroupId = GroupId(3);
const DOMAIN: u32 = 1;
const SERVER: u32 = 101;
const DC: &str = "dc-a";

fn schema(shard: u32) -> String {
    shard_schema(TENANT, DB, GROUP, ShardId(shard))
}

/// The routing hash of key `k` written in generation `g` (a key change
/// gives a row a new hash).
fn mix(k: i64, g: u64) -> u64 {
    let mut z = u64::from_ne_bytes(k.to_ne_bytes())
        .wrapping_add(g.wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// One stored row: value and routing hash.
type Val = (String, u64);
type Table = BTreeMap<i64, Val>;
/// One row change: table, before, after.
type Change = (String, Option<(i64, Val)>, Option<(i64, Val)>);

#[derive(Debug, Clone)]
enum Entry {
    /// Client rows on a schema.
    Rows(String, Vec<Change>),
    Wm(Watermark),
    /// DDL on a schema.
    Ddl(String, String),
    /// A mover's write into new shards (its own transaction).
    Apply,
}

#[derive(Debug, Default)]
struct Db {
    schemas: BTreeMap<String, BTreeMap<String, Table>>,
    binlog: Vec<Entry>,
    purged: u64,
    fenced: BTreeSet<String>,
    versions: BTreeSet<(String, i64, String, u64)>,
    pos: BTreeMap<JobId, PosRow>,
    /// The last acknowledged write per key: `None` for a delete.
    acked: BTreeMap<(String, i64), Option<Val>>,
    ddl_seq: u64,
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

fn table_meta(schema: &str, name: &str) -> TableMeta {
    let col = |n: &str| ColumnMeta {
        name: n.into(),
        generated: false,
        numeric: false,
    };
    TableMeta {
        schema: schema.to_owned(),
        name: name.to_owned(),
        columns: vec![col("k"), col("v"), col("_sprig_rk")],
        pk: vec![0],
        create_sql: String::new(),
    }
}

fn row(k: i64, v: &Val) -> Row {
    vec![
        Value::Int(k),
        Value::Bytes(v.0.as_bytes().to_vec()),
        Value::UInt(v.1),
    ]
}

fn parse_row(r: &Row) -> Option<(i64, Val)> {
    match (r.first()?, r.get(1)?, r.get(2)?) {
        (Value::Int(k), Value::Bytes(v), Value::UInt(h)) => {
            Some((*k, (String::from_utf8_lossy(v).into_owned(), *h)))
        }
        _ => None,
    }
}

fn key_of(after: Option<&Row>) -> Option<i64> {
    match after?.first()? {
        Value::Int(k) => Some(*k),
        _ => None,
    }
}

fn digest(t: &TableMeta, k: i64, v: &Val) -> RowDigest {
    let r = row(k, v);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in k
        .to_be_bytes()
        .iter()
        .chain(v.0.as_bytes())
        .chain(&v.1.to_be_bytes())
    {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    RowDigest {
        key: t.key_of(&r),
        key_values: vec![Value::Int(k)],
        hash: h,
    }
}

/// Fault rates for one run.
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

async fn io_call(world: &World, p: f64, what: &str) -> Result<()> {
    world.chaos_yield().await;
    tokio::time::sleep(Duration::from_micros(200 + world.rng().below(800))).await;
    if world.rng().chance(p) {
        world.record(format!("fault {what}"));
        return Err(fault(what));
    }
    Ok(())
}

/// One job's sources: the shard schemas its spec names.
struct SimSource {
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    world: World,
    rates: Rates,
    job: JobId,
    schemas: Vec<String>,
    tables: Vec<String>,
}

impl SimSource {
    fn rows<'a>(db: &'a Db, t: &TableMeta) -> Option<&'a Table> {
        db.schemas.get(&t.schema).and_then(|s| s.get(&t.name))
    }
}

#[async_trait::async_trait]
impl SourceIo for SimSource {
    async fn schemas(&self) -> Result<Vec<String>> {
        Ok(self.schemas.clone())
    }

    async fn tables(&self) -> Result<Vec<TableMeta>> {
        io_call(&self.world, self.rates.source_err, "source tables").await?;
        Ok(self
            .schemas
            .iter()
            .flat_map(|s| self.tables.iter().map(move |t| table_meta(s, t)))
            .collect())
    }

    async fn objects(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn triggers(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn binlog_pos(&self) -> Result<GtidPos> {
        io_call(&self.world, self.rates.source_err, "binlog_pos").await?;
        Ok(lock(&self.db).pos())
    }

    async fn open_stream(
        &self,
        from: &GtidPos,
        tables: Arc<Vec<Arc<TableMeta>>>,
    ) -> Result<Box<dyn ChangeStream>> {
        io_call(&self.world, self.rates.source_err, "open_stream").await?;
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
            schemas: self.schemas.clone(),
            job: self.job,
            drop: self.rates.stream_drop,
        }))
    }

    async fn write_watermark(&self, wm: &Watermark) -> Result<()> {
        io_call(&self.world, self.rates.source_err, "watermark").await?;
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
        io_call(&self.world, self.rates.source_err, "read_chunk").await?;
        let db = lock(&self.db);
        let lo = key_of(after);
        Ok(Self::rows(&db, t)
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
        io_call(&self.world, self.rates.source_err, "source digests").await?;
        let db = lock(&self.db);
        let lo = key_of(after);
        Ok(Self::rows(&db, t)
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
        io_call(&self.world, self.rates.source_err, "read_range").await?;
        let db = lock(&self.db);
        let lo = key_of(after);
        let hi = key_of(Some(last)).unwrap_or(i64::MAX);
        Ok(Self::rows(&db, t)
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
        io_call(&self.world, self.rates.source_err, "fence").await?;
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
        let mut s = db.seq();
        for sc in &self.schemas {
            db.fenced.insert(sc.clone());
            s = db.log(Entry::Ddl(
                sc.clone(),
                format!("RENAME TABLE `{sc}`.`a` TO `{sc}`.`__sprig_f_0`"),
            ));
        }
        self.notify.notify_waiters();
        self.world.record(format!("sources fenced at {s}"));
        Ok(Fenced {
            receipt: serde_json::json!({ "sim": s }),
            pos: GtidPos::from_gtids([gtid(s)]),
        })
    }

    async fn fence_reads(&self, _receipt: &serde_json::Value) -> Result<()> {
        Ok(())
    }

    async fn unfence(&self, _receipt: &serde_json::Value) -> Result<()> {
        io_call(&self.world, self.rates.source_err, "unfence").await?;
        self.unfence_lost().await
    }

    async fn unfence_lost(&self) -> Result<()> {
        let mut db = lock(&self.db);
        for sc in &self.schemas {
            if db.fenced.remove(sc) {
                db.log(Entry::Ddl(
                    sc.clone(),
                    format!("RENAME TABLE `{sc}`.`__sprig_f_0` TO `{sc}`.`a`"),
                ));
                self.world.record(format!("{sc} unfenced"));
            }
        }
        self.notify.notify_waiters();
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
        for sc in &self.schemas {
            db.schemas.remove(sc);
        }
        self.world.record(format!("dropped {:?}", self.schemas));
        Ok(())
    }
}

/// A stream of the cluster's binlog, filtered to one job's schemas.
struct SimStream {
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    world: World,
    next: u64,
    tables: Arc<Vec<Arc<TableMeta>>>,
    schemas: Vec<String>,
    job: JobId,
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
                        Entry::Rows(sc, changes) if self.schemas.contains(&sc) => {
                            for (t, b, a) in changes {
                                let Some(meta) =
                                    self.tables.iter().find(|m| m.schema == sc && m.name == t)
                                else {
                                    continue;
                                };
                                txn.changes.push(RowChange {
                                    table: meta.clone(),
                                    before: b.map(|(k, v)| row(k, &v)),
                                    after: a.map(|(k, v)| row(k, &v)),
                                });
                            }
                        }
                        Entry::Wm(w) if w.job == self.job => txn.watermarks.push(w),
                        Entry::Ddl(sc, sql) if self.schemas.contains(&sc) => {
                            txn.ddl = Some(SourceDdl { db: sc, sql });
                        }
                        _ => {} // another schema's, or a mover's write
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

/// One job's targets: new shard schemas on the same cluster, written
/// through the production routing.
struct SimTarget {
    db: Arc<Mutex<Db>>,
    world: World,
    rates: Rates,
    job: JobId,
    route: Arc<ShardRoute>,
}

impl SimTarget {
    fn check_token(&self, db: &Db, token: u64) -> Result<()> {
        match db.pos.get(&self.job) {
            Some(p) if p.token == token => Ok(()),
            _ => Err(SprigError::new(SprigCode::JobLeaseLost, "stale token")),
        }
    }
}

#[async_trait::async_trait]
impl TargetIo for SimTarget {
    async fn prepare(&self, tables: &[TableMeta], _objects: &[String]) -> Result<()> {
        io_call(&self.world, self.rates.target_err_before, "prepare").await?;
        let mut db = lock(&self.db);
        for t in tables {
            for s in self.route.targets_of(&t.schema) {
                db.schemas
                    .entry(s)
                    .or_default()
                    .entry(t.name.clone())
                    .or_default();
            }
        }
        db.log(Entry::Apply);
        Ok(())
    }

    async fn load_pos(&self, job: JobId) -> Result<Option<PosRow>> {
        io_call(&self.world, self.rates.target_err_before, "load_pos").await?;
        Ok(lock(&self.db).pos.get(&job).cloned())
    }

    async fn claim(&self, job: JobId, token: u64, init: &PosRow) -> Result<PosRow> {
        io_call(&self.world, self.rates.target_err_before, "claim").await?;
        let mut db = lock(&self.db);
        match db.pos.get_mut(&job) {
            None => {
                let mut row = init.clone();
                row.token = token;
                db.pos.insert(job, row.clone());
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
        io_call(&self.world, self.rates.target_err_before, "apply").await?;
        let routed = self.route.route_ops(ops)?;
        {
            let mut db = lock(&self.db);
            self.check_token(&db, token)?;
            for op in &routed {
                match op {
                    ApplyOp::Upsert { table, rows } => {
                        for r in rows {
                            let Some((k, v)) = parse_row(r) else { continue };
                            if !db
                                .versions
                                .contains(&(table.name.clone(), k, v.0.clone(), v.1))
                            {
                                self.world.fail(format!(
                                    "invariant 2: target {}.{} got {k} = {v:?}, a version the \
                                     source never had",
                                    table.schema, table.name
                                ));
                            }
                            db.schemas
                                .entry(table.schema.clone())
                                .or_default()
                                .entry(table.name.clone())
                                .or_default()
                                .insert(k, v);
                        }
                    }
                    ApplyOp::Delete { table, keys } => {
                        for kv in keys {
                            if let Some(k) = key_of(Some(kv))
                                && let Some(t) = db
                                    .schemas
                                    .get_mut(&table.schema)
                                    .and_then(|s| s.get_mut(&table.name))
                            {
                                t.remove(&k);
                            }
                        }
                    }
                }
            }
            db.log(Entry::Apply);
            if let Some(p) = db.pos.get_mut(&self.job) {
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
        io_call(&self.world, self.rates.target_err_before, "target digests").await?;
        let db = lock(&self.db);
        let lo = key_of(after);
        let hi = key_of(Some(last)).unwrap_or(i64::MAX);
        let mut all = Vec::new();
        for s in self.route.targets_of(&t.schema) {
            let tt = ShardRoute::retarget(t, &s);
            if let Some(rows) = db.schemas.get(&s).and_then(|x| x.get(&t.name)) {
                for (k, v) in rows {
                    if lo.is_none_or(|lo| *k > lo) && *k <= hi {
                        all.push((s.clone(), digest(&tt, *k, v), Some(v.1)));
                    }
                }
            }
        }
        Ok(self.route.merge_digests(t, all))
    }

    async fn delete_keys(&self, job: JobId, token: u64, t: &TableMeta, keys: &[Row]) -> Result<()> {
        let pos = lock(&self.db)
            .pos
            .get(&self.job)
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
        io_call(&self.world, self.rates.target_err_before, "counters").await
    }

    async fn finalize(&self, _triggers: &[String]) -> Result<GtidPos> {
        io_call(&self.world, self.rates.target_err_before, "finalize").await?;
        Ok(lock(&self.db).pos())
    }

    async fn unfinalize(&self, _triggers: &[String]) -> Result<()> {
        Ok(())
    }

    async fn reset(&self, job: JobId, _keep_schemas: bool) -> Result<()> {
        io_call(&self.world, self.rates.target_err_before, "reset").await?;
        let mut db = lock(&self.db);
        if db.pos.remove(&job).is_some() {
            for s in self.route.target_schemas() {
                db.schemas.remove(&s);
            }
        }
        db.log(Entry::Apply);
        self.world.record("targets reset");
        Ok(())
    }

    async fn forget(&self, job: JobId) -> Result<()> {
        let mut db = lock(&self.db);
        db.pos.remove(&job);
        db.log(Entry::Apply);
        Ok(())
    }

    async fn load(&self) -> Result<Load> {
        Ok(Load::default())
    }
}

/// The production group map commit, with invariants 3, 4 and 6 checked
/// around it.
struct CheckedMaps {
    inner: GroupMapIo,
    db: Arc<Mutex<Db>>,
    world: World,
    clock: Arc<SimClock>,
    held_at: Mutex<Option<Instant>>,
    budget: Duration,
    route: Arc<ShardRoute>,
}

impl CheckedMaps {
    fn before_commit(&self) {
        let db = lock(&self.db);
        for src in &self.route.sources {
            if !db.fenced.contains(&src.schema) {
                self.world.fail(format!(
                    "invariant 3: commit while {} is unfenced",
                    src.schema
                ));
            }
        }
        let empty = BTreeMap::new();
        for t in self.route.target_schemas() {
            let mut expect: BTreeMap<String, Table> = BTreeMap::new();
            let mut range = None;
            for src in &self.route.sources {
                for tr in src.targets.iter().filter(|x| x.schema == t) {
                    range = Some(tr.range);
                    for (name, rows) in db.schemas.get(&src.schema).unwrap_or(&empty) {
                        let e = expect.entry(name.clone()).or_default();
                        for (k, v) in rows {
                            if tr.range.contains(v.1) {
                                e.insert(*k, v.clone());
                            }
                        }
                    }
                }
            }
            let got = db.schemas.get(&t).unwrap_or(&empty);
            for (name, rows) in &expect {
                let have = got.get(name).cloned().unwrap_or_default();
                if &have != rows {
                    self.world.fail(format!(
                        "invariant 4: at commit, {t}.{name} has {} rows, its sources {} ({:?})",
                        have.len(),
                        rows.len(),
                        range
                    ));
                }
            }
            if let Some(r) = range {
                for (name, rows) in got {
                    if let Some((k, _)) = rows.iter().find(|(_, v)| !r.contains(v.1)) {
                        self.world.fail(format!(
                            "invariant 4: at commit, {t}.{name} holds {k}, outside its range"
                        ));
                    }
                }
            }
        }
    }

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

struct Factory {
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    world: World,
    node: Node,
    rates: Rates,
    tables: Vec<String>,
    settings: MoverSettings,
}

fn tier() -> Result<Tier> {
    Ok(Tier::Dc(
        sprig_common::ids::DcId::new(DC).map_err(|e| SprigError::internal(e.to_string()))?,
    ))
}

#[async_trait::async_trait]
impl EnvFactory for Factory {
    async fn env(&self, rec: &JobRecord) -> Result<JobEnv> {
        let meta = self.world.etcd().client(&self.node, tier()?);
        let route = Arc::new(
            ShardRoute::from_spec(&rec.spec)?
                .ok_or_else(|| SprigError::internal("not a hash-range job"))?,
        );
        let schemas: Vec<String> = rec
            .spec
            .sources
            .iter()
            .flat_map(|s| s.schemas.iter().cloned())
            .collect();
        Ok(JobEnv {
            source: Arc::new(SimSource {
                db: self.db.clone(),
                notify: self.notify.clone(),
                world: self.world.clone(),
                rates: self.rates,
                job: rec.job_id,
                schemas,
                tables: self.tables.clone(),
            }),
            target: Arc::new(SimTarget {
                db: self.db.clone(),
                world: self.world.clone(),
                rates: self.rates,
                job: rec.job_id,
                route: route.clone(),
            }),
            maps: Arc::new(CheckedMaps {
                inner: GroupMapIo::new(meta, &rec.spec)?,
                db: self.db.clone(),
                world: self.world.clone(),
                clock: self.world.clock(&self.node),
                held_at: Mutex::new(None),
                budget: self.settings.cutover_max_hold + self.settings.hold_grace,
                route,
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

/// What a client's attempt did.
enum Step {
    /// Held, fenced or dropped: reload the map and retry.
    Wait,
    /// Acknowledged.
    Wrote,
    /// Nothing to do.
    Skip,
}

/// One client: routes each write by the group's map like a gateway.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn client(
    world: World,
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    maps: ShardMapStore,
    me: usize,
    of: usize,
    tables: Vec<String>,
    rows: i64,
    stop: CancellationToken,
) {
    let node = Node::new(format!("gw{me}"));
    let clock = world.clock(&node);
    let late = world.rng().chance(0.3);
    let mut cached: Option<(ShardMap, Instant)> = None;
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
        let rekey = world.rng().chance(0.15);
        let v = format!("c{me}-{n}");
        loop {
            if stop.is_cancelled() {
                return;
            }
            let stale = cached.as_ref().is_some_and(|(_, at)| {
                clock.now().saturating_duration_since(*at) > Duration::from_millis(200)
            });
            if cached.is_none() || stale {
                let Ok((m, _)) = maps.load(TENANT, GROUP).await else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                cached = Some((m, clock.now()));
            }
            let Some((m, _)) = cached.clone() else {
                continue;
            };
            let now_ms = sprig_mover::record::wall_ms(clock.wall());
            let attempt = 'locked: {
                let mut d = lock(&db);
                let old = d.acked.get(&(t.clone(), k)).cloned().flatten();
                // The write's routing hash: a new key keeps its first hash; a
                // key change stays in its shard (a move between shards is the
                // router's distributed transaction, not modelled here).
                let rk = match &old {
                    Some((_, h)) if rekey => {
                        let new = mix(k, n);
                        if m.route(sprig_shard::RoutingHash(new)).shard
                            == m.route(sprig_shard::RoutingHash(*h)).shard
                        {
                            new
                        } else {
                            *h
                        }
                    }
                    Some((_, h)) => *h,
                    None => mix(k, 0),
                };
                let range = m.route(sprig_shard::RoutingHash(rk));
                let held = range.state == RangeState::Held
                    && range.hold_until_ms.is_some_and(|u| now_ms < u);
                if held && !late {
                    break 'locked Step::Wait;
                }
                let sc = schema(range.shard.0);
                if d.fenced.contains(&sc) || !d.schemas.contains_key(&sc) {
                    // A fence error or a dropped schema: reload the map.
                    break 'locked Step::Wait;
                }
                if ddl {
                    d.ddl_seq += 1;
                    let s = d.ddl_seq;
                    d.log(Entry::Ddl(
                        sc.clone(),
                        format!("ALTER TABLE `{sc}`.`{t}` COMMENT 'c{s}'"),
                    ));
                    break 'locked Step::Wrote;
                }
                if del && old.is_none() {
                    break 'locked Step::Skip;
                }
                let tbl = d
                    .schemas
                    .get_mut(&sc)
                    .map(|s| s.entry(t.clone()).or_default());
                let Some(tbl) = tbl else {
                    break 'locked Step::Skip;
                };
                let before = tbl.get(&k).cloned();
                let after = (!del).then(|| (v.clone(), rk));
                match &after {
                    None => {
                        tbl.remove(&k);
                    }
                    Some(val) => {
                        tbl.insert(k, val.clone());
                    }
                }
                if let Some(val) = &after {
                    d.versions.insert((t.clone(), k, val.0.clone(), val.1));
                }
                if before.is_some() || after.is_some() {
                    d.log(Entry::Rows(
                        sc.clone(),
                        vec![(
                            t.clone(),
                            before.map(|b| (k, b)),
                            after.clone().map(|a| (k, a)),
                        )],
                    ));
                }
                d.acked.insert((t.clone(), k), after);
                Step::Wrote
            };
            match attempt {
                Step::Wait => {
                    cached = None;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Step::Wrote => notify.notify_waiters(),
                Step::Skip => {}
            }
            break;
        }
    }
}

/// What one run does.
#[derive(Debug, Clone)]
pub struct SplitScenario {
    /// Tables in the group.
    pub tables: usize,
    /// Rows per table at the start.
    pub rows: i64,
    /// Concurrent clients.
    pub clients: usize,
    /// Inject faults.
    pub faults: bool,
    /// The split point, as a fraction of the hash space in thousandths.
    pub split_at: u64,
}

impl SplitScenario {
    /// A scenario drawn from the seed.
    #[must_use]
    pub fn random(rng: &SimRng) -> Self {
        Self {
            tables: 1 + rng.index(2),
            rows: i64::try_from(rng.below(41)).unwrap_or(0),
            clients: 1 + rng.index(3),
            faults: !rng.chance(0.1),
            split_at: 100 + rng.below(800),
        }
    }
}

/// Starts a worker on `n`.
struct Workers {
    world: World,
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    rates: Rates,
    tables: Vec<String>,
    settings: MoverSettings,
    stop: CancellationToken,
    running: BTreeMap<String, (tokio::task::JoinHandle<Result<()>>, CancellationToken)>,
}

impl Workers {
    fn start(&mut self, n: &Node) -> Result<()> {
        let factory = Arc::new(Factory {
            db: self.db.clone(),
            notify: self.notify.clone(),
            world: self.world.clone(),
            node: n.clone(),
            rates: self.rates,
            tables: self.tables.clone(),
            settings: self.settings.clone(),
        });
        let clock: Arc<dyn Clock> = self.world.clock(n);
        let store = JobStore::new(self.world.etcd().client(n, tier()?), clock);
        let mut opts = WorkerOpts::new(&n.0);
        opts.lease = self.settings.job_lease;
        opts.scan_interval = Duration::from_millis(500);
        let rt = JobRuntime::new(store, factory, opts);
        let tok = self.stop.child_token();
        let t = tok.clone();
        self.running.insert(
            n.0.clone(),
            (tokio::spawn(async move { rt.run(t).await }), tok),
        );
        Ok(())
    }
}

/// Runs one job to its end with faults; returns its final state.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn drive(
    world: &World,
    rng: &SimRng,
    faults: bool,
    mover: &MetaMover,
    workers: &mut Workers,
    nodes: &[Node],
    db: &Arc<Mutex<Db>>,
    id: JobId,
) -> std::result::Result<JobState, String> {
    let crash_states = [
        JobState::Preparing,
        JobState::Copying,
        JobState::CatchingUp,
        JobState::Verifying,
        JobState::Ready,
        JobState::CuttingOver,
        JobState::Retaining,
    ];
    let crash_at = faults.then(|| *rng.pick(&crash_states));
    let mut crashed = false;
    let partition_at = (faults && rng.chance(0.3))
        .then(|| rng.duration(Duration::from_secs(1), Duration::from_secs(20)));
    let mut partitioned = None;
    let purge_at = (faults && rng.chance(0.2))
        .then(|| rng.duration(Duration::from_millis(200), Duration::from_secs(10)));
    let deadline = Duration::from_secs(1200);
    let started = world.elapsed();
    let end = loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = world.elapsed().saturating_sub(started);
        if now > deadline {
            return Err(format!(
                "invariant 7: job {} didn't end within {deadline:?}",
                id.0
            ));
        }
        let Ok(st) = mover.status(&id).await else {
            continue;
        };
        if st.state.is_terminal() || st.state == JobState::Paused {
            break st;
        }
        if let Some(cs) = crash_at
            && !crashed
            && st.state == cs
            && rng.chance(0.2)
        {
            crashed = true;
            let owner = mover.store().get(&id).await.ok().and_then(|(r, _)| r.owner);
            if let Some(o) = owner
                && let Some((h, tok)) = workers.running.remove(&o)
            {
                world.record(format!("crash worker {o} in {}", st.state));
                tok.cancel();
                h.abort();
                let n = Node::from(o.as_str());
                let restart = rng.duration(Duration::from_millis(100), Duration::from_secs(5));
                tokio::time::sleep(restart).await;
                workers.start(&n).map_err(|e| e.to_string())?;
            }
        }
        if let Some(at) = partition_at
            && partitioned.is_none()
            && now >= at
        {
            let w = rng.pick(nodes).clone();
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
            let mut d = lock(db);
            if !st.state.is_committed() && d.purged < d.seq() && rng.chance(0.05) {
                d.purged = d.seq();
                world.record(format!("binlog purged to {}", d.purged));
            }
        }
    };
    if let Some((f, _)) = partitioned {
        world.net().heal(f);
    }
    match end.state {
        JobState::Done | JobState::RolledBack => Ok(end.state),
        s => Err(format!("invariant 7: job ended {s}: {:?}", end.error)),
    }
}

/// Runs one seed: a split under load and faults, then a merge back, then
/// the checks.
///
/// # Errors
/// The first broken invariant.
#[allow(clippy::too_many_lines)]
pub async fn run_split_merge(world: World) -> std::result::Result<(), String> {
    let rng = world.rng().fork();
    let sc = SplitScenario::random(&rng);
    let rates = Rates::draw(&rng, sc.faults);
    world.record(format!("scenario {sc:?} rates {rates:?}"));
    let tier = tier().map_err(|e| e.to_string())?;
    let c1 = cluster("dc-a-c1");
    let tables: Vec<String> = ["a", "b"][..sc.tables]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    let db = Arc::new(Mutex::new(Db::default()));
    let notify = Arc::new(Notify::new());
    {
        let mut d = lock(&db);
        let s1 = schema(1);
        for t in &tables {
            let mut changes = Vec::new();
            let mut tbl = Table::new();
            for k in 0..sc.rows {
                let v = (format!("init-{k}"), mix(k, 0));
                tbl.insert(k, v.clone());
                d.versions.insert((t.clone(), k, v.0.clone(), v.1));
                d.acked.insert((t.clone(), k), Some(v.clone()));
                changes.push((t.clone(), None, Some((k, v))));
            }
            d.schemas
                .entry(s1.clone())
                .or_default()
                .insert(t.clone(), tbl);
            d.log(Entry::Rows(s1.clone(), changes));
        }
    }
    let admin = world.etcd().client(&Node::from("admin"), tier.clone());
    let maps = ShardMapStore::new(admin.clone());
    let m0 = ShardMap::presplit(TENANT, GROUP, 7, 1, std::slice::from_ref(&c1))
        .map_err(|e| e.to_string())?;
    maps.create(&m0).await.map_err(|e| e.to_string())?;

    let mut settings = MoverSettings::for_tests();
    settings.job_lease = Duration::from_secs(3);
    settings.source_retain = Duration::from_secs(1);
    let admin_clock: Arc<dyn Clock> = world.clock(&Node::from("admin"));
    let mover = MetaMover::new(JobStore::new(admin.clone(), admin_clock));

    let stop = CancellationToken::new();
    let nodes: Vec<Node> = vec![Node::from("w1"), Node::from("w2")];
    let mut workers = Workers {
        world: world.clone(),
        db: db.clone(),
        notify: notify.clone(),
        rates,
        tables: tables.clone(),
        settings: settings.clone(),
        stop: stop.clone(),
        running: BTreeMap::new(),
    };
    for n in &nodes {
        workers.start(n).map_err(|e| e.to_string())?;
    }
    let mut clients = Vec::new();
    for i in 0..sc.clients {
        let node = Node::new(format!("gw{i}"));
        let m = ShardMapStore::new(world.etcd().client(&node, tier.clone()));
        clients.push(tokio::spawn(client(
            world.clone(),
            db.clone(),
            notify.clone(),
            m,
            i,
            sc.clients,
            tables.clone(),
            sc.rows,
            stop.clone(),
        )));
    }

    // 1. Split shard 1 at the scenario's point into shards 2 and 3.
    let mid = u64::try_from((u128::from(sc.split_at) << 64) / 1000).unwrap_or(1 << 63);
    let split = JobSpec::reshard(
        TENANT,
        GROUP.0,
        &c1,
        vec![(1, vec![schema(1)], HashRange { lo: 0, hi: None })],
        &c1,
        vec![
            (
                2,
                HashRange {
                    lo: 0,
                    hi: Some(mid),
                },
            ),
            (3, HashRange { lo: mid, hi: None }),
        ],
    );
    let id = mover.submit(split).await.map_err(|e| e.to_string())?;
    let first = drive(
        &world,
        &rng,
        sc.faults,
        &mover,
        &mut workers,
        &nodes,
        &db,
        id,
    )
    .await?;
    world.record(format!("split ended {first}"));
    let mut committed = u64::from(first == JobState::Done);
    let mut expect_dropped: Vec<String> = Vec::new();
    let mut expect_absent: Vec<String> = Vec::new();
    if first == JobState::Done {
        expect_dropped.push(schema(1));
        // 2. Merge them back into shard 4.
        let merge = JobSpec::reshard(
            TENANT,
            GROUP.0,
            &c1,
            vec![
                (
                    2,
                    vec![schema(2)],
                    HashRange {
                        lo: 0,
                        hi: Some(mid),
                    },
                ),
                (3, vec![schema(3)], HashRange { lo: mid, hi: None }),
            ],
            &c1,
            vec![(4, HashRange { lo: 0, hi: None })],
        );
        let id = mover.submit(merge).await.map_err(|e| e.to_string())?;
        let second = drive(
            &world,
            &rng,
            sc.faults,
            &mover,
            &mut workers,
            &nodes,
            &db,
            id,
        )
        .await?;
        world.record(format!("merge ended {second}"));
        if second == JobState::Done {
            committed += 1;
            expect_dropped.extend([schema(2), schema(3)]);
        } else {
            expect_absent.push(schema(4));
        }
    } else {
        expect_absent.extend([schema(2), schema(3)]);
    }
    stop.cancel();
    for c in clients {
        let _ = c.await;
    }
    for (_, (h, _)) in std::mem::take(&mut workers.running) {
        h.abort();
    }

    // The checks.
    let (m, _) = maps.load(TENANT, GROUP).await.map_err(|e| e.to_string())?;
    m.validate().map_err(|e| format!("invariant 5: {e}"))?;
    if let Some(r) = m.ranges.iter().find(|r| r.state != RangeState::Serving) {
        return Err(format!("invariant 5: shard {} is still held", r.shard));
    }
    let top = m.ranges.iter().map(|r| r.epoch).max().unwrap_or(0);
    if top != 1 + committed {
        return Err(format!(
            "invariant 5: epoch {top} after {committed} committed jobs"
        ));
    }
    let d = lock(&db);
    for s in &expect_dropped {
        if d.schemas.contains_key(s) {
            return Err(format!("invariant 7: retired {s} wasn't dropped"));
        }
    }
    for s in &expect_absent {
        if d.schemas.contains_key(s) {
            return Err(format!(
                "invariant 7: rolled-back target {s} wasn't dropped"
            ));
        }
    }
    let serving: Vec<(String, HashRange)> = m
        .ranges
        .iter()
        .enumerate()
        .map(|(i, r)| {
            (
                schema(r.shard.0),
                HashRange {
                    lo: r.lo,
                    hi: m.hi(i),
                },
            )
        })
        .collect();
    for (s, r) in &serving {
        if !d.schemas.contains_key(s) {
            return Err(format!("invariant 1: serving shard {s} has no schema"));
        }
        for (t, rows) in &d.schemas[s] {
            for (k, v) in rows {
                if !r.contains(v.1) {
                    return Err(format!("invariant 1: {s}.{t}.{k} is outside {r:?}"));
                }
                if d.acked.get(&(t.clone(), *k)) != Some(&Some(v.clone())) {
                    return Err(format!(
                        "invariant 1: {s}.{t}.{k} = {v:?} was never acknowledged last ({:?})",
                        d.acked.get(&(t.clone(), *k))
                    ));
                }
            }
        }
    }
    for ((t, k), v) in &d.acked {
        let holders: Vec<&String> = serving
            .iter()
            .filter(|(s, _)| {
                d.schemas
                    .get(s)
                    .and_then(|x| x.get(t))
                    .is_some_and(|x| x.contains_key(k))
            })
            .map(|(s, _)| s)
            .collect();
        match v {
            None if !holders.is_empty() => {
                return Err(format!("invariant 1: deleted {t}.{k} is in {holders:?}"));
            }
            Some(val) => {
                let want = schema(m.route(sprig_shard::RoutingHash(val.1)).shard.0);
                if holders != vec![&want] {
                    return Err(format!(
                        "invariant 1: {t}.{k} = {val:?} is in {holders:?}, expected {want}"
                    ));
                }
            }
            None => {}
        }
    }
    drop(d);
    if let Some(f) = world.failures().first() {
        return Err(f.clone());
    }
    Ok(())
}
