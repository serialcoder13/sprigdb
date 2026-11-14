//! The feed handoff under simulation (the mover's invariant 8).
//!
//! The real tenant feed ([`sprig_feed::TenantTail`]) reads the two
//! clusters' binlog models while the real mover moves the tenant. The
//! target's binlog holds the mover's applies (each with its position row),
//! its table creation, and after the cutover the clients' writes. A
//! consumer reads the feed, makes batches durable at random points and
//! crashes at random times, then resumes after its last durable token,
//! also across the move. At the end, its durable events must be exactly
//! the clients' committed changes: the source's in commit order, then the
//! target's, none missing, none twice, none of the mover's copies, and
//! no physical schema name in any event.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_binlog::{Gtid, GtidPos, RowImage, Value as BValue};
use sprig_common::ids::{ClusterId, DbId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_feed::names::StaticResolver;
use sprig_feed::source::{
    Clusters, DumpScope, RawDdl, RawRow, RawTxn, RowKind, SourceItem, TableShape, TxnSource,
};
use sprig_feed::{FeedItem, MetaPlacement, Op, ResumeToken, StartAt, TailOptions, TenantTail};
use sprig_meta::Tier;
use tokio::sync::Notify;

use super::{DOMAIN, Db, Entry, ModelChange, SCHEMA, SERVER, TENANT, TgtEntry, lock};
use crate::faults::Node;
use crate::sim::World;

/// The target's GTID domain and server.
pub(super) const TGT_DOMAIN: u32 = 2;
const TGT_SERVER: u32 = 201;

pub(super) fn tgt_gtid(seq: u64) -> Gtid {
    Gtid {
        domain: TGT_DOMAIN,
        server: TGT_SERVER,
        seq,
    }
}

fn gtid_of(src: bool, seq: u64) -> Gtid {
    if src {
        Gtid {
            domain: DOMAIN,
            server: SERVER,
            seq,
        }
    } else {
        tgt_gtid(seq)
    }
}

fn pos_of(src: bool, seq: u64) -> GtidPos {
    if seq == 0 {
        GtidPos::new()
    } else {
        GtidPos::from_gtids([gtid_of(src, seq)])
    }
}

fn shape(table: &str) -> Arc<TableShape> {
    Arc::new(TableShape {
        schema: SCHEMA.to_owned(),
        table: table.to_owned(),
        columns: vec!["k".into(), "v".into()],
        key: vec![0],
    })
}

fn image(k: i64, v: &str) -> RowImage {
    RowImage(vec![
        Some(BValue::Int(k)),
        Some(BValue::String(v.as_bytes().to_vec())),
    ])
}

fn rows_of(changes: &[ModelChange]) -> Vec<RawRow> {
    changes
        .iter()
        .map(|(t, b, a)| RawRow {
            table: shape(t),
            kind: match (b, a) {
                (None, _) => RowKind::Insert,
                (Some(_), Some(_)) => RowKind::Update,
                (Some(_), None) => RowKind::Delete,
            },
            before: b.as_ref().map(|(k, v)| image(*k, v)),
            after: a.as_ref().map(|(k, v)| image(*k, v)),
            hint: None,
        })
        .collect()
}

/// The two clusters' binlogs as [`Clusters`].
pub(super) struct SimClusters {
    pub db: Arc<Mutex<Db>>,
    pub notify: Arc<Notify>,
    pub world: World,
    pub drop: f64,
    pub src: ClusterId,
    pub tgt: ClusterId,
}

struct SimTxns {
    db: Arc<Mutex<Db>>,
    notify: Arc<Notify>,
    world: World,
    drop: f64,
    src: bool,
    next: u64,
    /// A cluster-wide dump's scope (the shared reader's).
    scope: Option<Arc<DumpScope>>,
}

/// What a dump with `scope` keeps of a group (as `BinlogTxns` does).
fn cut(mut t: RawTxn, scope: &DumpScope) -> RawTxn {
    t.rows
        .retain(|r| scope.keeps(&r.table.schema, &r.table.table));
    t.ddl = t.ddl.filter(|d| scope.names_whole(&d.db, &d.sql));
    t
}

#[async_trait::async_trait]
impl TxnSource for SimTxns {
    async fn next(&mut self) -> Result<SourceItem> {
        loop {
            let notified = self.notify.notified();
            let txn = {
                let db = lock(&self.db);
                let i = usize::try_from(self.next - 1).unwrap_or(usize::MAX);
                let seq = self.next;
                if self.src {
                    db.binlog.get(i).map(|e| {
                        let mut t = RawTxn::new(gtid_of(true, seq), pos_of(true, seq - 1), ts(seq));
                        match e {
                            Entry::Rows(c) => t.rows = rows_of(c),
                            Entry::Wm(_) => {}
                            Entry::Ddl(sql) => {
                                t.ddl = Some(RawDdl {
                                    db: SCHEMA.to_owned(),
                                    sql: sql.clone(),
                                });
                            }
                        }
                        t
                    })
                } else {
                    db.tgt_binlog.get(i).map(|e| {
                        let mut t =
                            RawTxn::new(gtid_of(false, seq), pos_of(false, seq - 1), ts(seq));
                        match e {
                            TgtEntry::Apply(c) => {
                                t.rows = rows_of(c);
                                t.apply = true;
                            }
                            TgtEntry::Client(c) => t.rows = rows_of(std::slice::from_ref(c)),
                            TgtEntry::Ddl(sql) => {
                                t.ddl = Some(RawDdl {
                                    db: SCHEMA.to_owned(),
                                    sql: sql.clone(),
                                });
                            }
                        }
                        t
                    })
                }
            };
            if let Some(t) = txn {
                let t = match &self.scope {
                    Some(s) => cut(t, s),
                    None => t,
                };
                self.world.chaos_yield().await;
                if self.world.rng().chance(self.drop) {
                    self.world.record("fault feed stream drop");
                    return Err(SprigError::new(SprigCode::BinlogDisconnected, "injected"));
                }
                self.next += 1;
                return Ok(SourceItem::Txn(t));
            }
            tokio::select! {
                biased;
                () = notified => {}
                () = tokio::time::sleep(Duration::from_secs(1)) => {
                    return Ok(SourceItem::Idle { pos: pos_of(self.src, self.next - 1) });
                }
            }
        }
    }
}

/// Event header seconds: a property of the entry, so re-reads match.
fn ts(seq: u64) -> u32 {
    1_800_000_000 + u32::try_from(seq / 4).unwrap_or(0)
}

#[async_trait::async_trait]
impl Clusters for SimClusters {
    async fn open(
        &self,
        cluster: &ClusterId,
        tenant: TenantId,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        assert_eq!(tenant, TENANT);
        self.world.chaos_yield().await;
        if self.world.rng().chance(self.drop) {
            return Err(SprigError::new(SprigCode::BinlogDisconnected, "injected"));
        }
        let src = *cluster == self.src;
        if !src && *cluster != self.tgt {
            return Err(SprigError::new(
                SprigCode::ClusterNotFound,
                cluster.to_string(),
            ));
        }
        let d = if src { DOMAIN } else { TGT_DOMAIN };
        let seq = from.get(d).map_or(0, |g| g.seq);
        // The feed's binlog retention covers the run: purges only cut the
        // mover's history (`feed.retention_hours`).
        Ok(Box::new(SimTxns {
            db: self.db.clone(),
            notify: self.notify.clone(),
            world: self.world.clone(),
            drop: self.drop,
            src,
            next: seq + 1,
            scope: None,
        }))
    }

    async fn open_scope(
        &self,
        cluster: &ClusterId,
        scope: Arc<DumpScope>,
        from: &GtidPos,
    ) -> Result<Box<dyn TxnSource>> {
        self.world.chaos_yield().await;
        if self.world.rng().chance(self.drop) {
            return Err(SprigError::new(SprigCode::BinlogDisconnected, "injected"));
        }
        let src = *cluster == self.src;
        if !src && *cluster != self.tgt {
            return Err(SprigError::new(
                SprigCode::ClusterNotFound,
                cluster.to_string(),
            ));
        }
        let d = if src { DOMAIN } else { TGT_DOMAIN };
        let seq = from.get(d).map_or(0, |g| g.seq);
        self.world.record(format!(
            "feed dump {} from {seq} ({})",
            if src { "src" } else { "tgt" },
            if scope.only.is_some() {
                "catch-up"
            } else {
                "shared"
            }
        ));
        Ok(Box::new(SimTxns {
            db: self.db.clone(),
            notify: self.notify.clone(),
            world: self.world.clone(),
            drop: self.drop,
            src,
            next: seq + 1,
            scope: Some(scope),
        }))
    }

    async fn position(&self, cluster: &ClusterId) -> Result<GtidPos> {
        let db = lock(&self.db);
        Ok(if *cluster == self.src {
            pos_of(true, db.binlog.len() as u64)
        } else {
            pos_of(false, db.tgt_binlog.len() as u64)
        })
    }
}

/// One durable event as the check compares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Seen {
    pub what: String,
    pub id: (String, u64, u32),
    pub clamp: u64,
}

fn kv(img: Option<&sprig_feed::Image>) -> Option<(i64, String)> {
    let img = img?;
    let k = match img.get("k")? {
        BValue::Int(k) => *k,
        _ => return None,
    };
    let v = match img.get("v")? {
        BValue::String(b) => String::from_utf8_lossy(b).into_owned(),
        _ => return None,
    };
    Some((k, v))
}

/// The expected form of a row change.
pub(super) fn row_text(t: &str, b: Option<&(i64, String)>, a: Option<&(i64, String)>) -> String {
    format!("{t} {b:?} -> {a:?}")
}

/// The expected form of a client DDL statement, with logical names.
pub(super) fn ddl_text(sql: &str) -> String {
    format!("ddl {}", sql.replace(&format!("`{SCHEMA}`"), "`shop`"))
}

/// What the consumer made durable.
#[derive(Debug, Default)]
pub(super) struct FeedLog {
    pub seen: Vec<Seen>,
    pub token: Option<ResumeToken>,
    pub restarts: u64,
    pub error: Option<String>,
}

/// Where the consumer must get to before it stops: the final owner and
/// its binlog length.
pub(super) type Goal = Arc<Mutex<Option<(ClusterId, u64)>>>;

pub(super) fn reached(
    token: Option<&ResumeToken>,
    goal: &(ClusterId, u64),
    tgt: &ClusterId,
) -> bool {
    let Some(t) = token else {
        return false;
    };
    let d = if &goal.0 == tgt { TGT_DOMAIN } else { DOMAIN };
    t.cluster == goal.0 && t.pos.get(d).map_or(0, |g| g.seq) >= goal.1
}

/// The consumer: reads the feed, commits batches at random, crashes at
/// random, resumes after its last durable token.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) async fn consumer(
    world: World,
    clusters: Arc<SimClusters>,
    tier: Tier,
    faults: bool,
    goal: Goal,
    log: Arc<Mutex<FeedLog>>,
    stop: CancellationToken,
) {
    let node = Node::from("feed");
    let placement = Arc::new(MetaPlacement::new(world.etcd().client(&node, tier)));
    let names = Arc::new(
        StaticResolver::new()
            .database(TENANT, DbId(1), "shop")
            .object(TENANT, DbId(1), "a", 11)
            .object(TENANT, DbId(1), "b", 12)
            .object(TENANT, DbId(1), "c", 13),
    );
    let rng = world.rng().fork();
    let opts = TailOptions {
        progress_every: Duration::from_millis(500),
        handoff_poll: Duration::from_millis(300),
        ..TailOptions::default()
    };
    'life: loop {
        if stop.is_cancelled() {
            return;
        }
        let start = log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .token
            .clone()
            .map_or_else(
                || StartAt::At {
                    cluster: clusters.src.clone(),
                    pos: GtidPos::new(),
                },
                StartAt::After,
            );
        let mut tail = match TenantTail::start(
            TENANT,
            start,
            clusters.clone(),
            placement.clone(),
            names.clone(),
            opts.clone(),
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                world.record(format!("feed start failed: {e}"));
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let crash_in = (faults && rng.chance(0.6))
            .then(|| rng.duration(Duration::from_millis(300), Duration::from_secs(15)));
        let crash = async {
            match crash_in {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(crash);
        let mut batch: Vec<Seen> = Vec::new();
        loop {
            let item = tokio::select! {
                biased;
                () = &mut crash => {
                    let mut l = log.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    l.restarts += 1;
                    world.record(format!("feed consumer crash, {} events lost", batch.len()));
                    continue 'life;
                }
                () = stop.cancelled() => return,
                r = tail.next() => r,
            };
            let item = match item {
                Ok(i) => i,
                Err(e) => {
                    let mut l = log
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    l.error = Some(e.to_string());
                    world.fail(format!("invariant 8: the feed failed: {e}"));
                    return;
                }
            };
            let commit = match &item {
                FeedItem::Event(e) => {
                    let text = format!("{e:?}");
                    if text.contains(SCHEMA) || text.contains("t1_d") || text.contains("__sprig") {
                        world.fail(format!(
                            "invariant 8: an event names a physical or internal object: {text}"
                        ));
                    }
                    let what = match e.op {
                        Op::Insert | Op::Update | Op::Delete => row_text(
                            &e.table,
                            kv(e.before.as_ref()).as_ref(),
                            kv(e.after.as_ref()).as_ref(),
                        ),
                        _ => format!("ddl {}", e.ddl.clone().unwrap_or_default()),
                    };
                    batch.push(Seen {
                        what,
                        id: (e.id.cluster.to_string(), e.id.gtid.seq, e.id.row),
                        clamp: e.hlc,
                    });
                    rng.chance(0.1)
                }
                FeedItem::TxnEnd { .. } => rng.chance(0.7),
                FeedItem::Progress { .. } => rng.chance(0.5),
            };
            if commit {
                let mut l = log
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                l.seen.append(&mut batch);
                l.token = Some(item.token().clone());
                let g = goal
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                if let Some(g) = g
                    && reached(l.token.as_ref(), &g, &clusters.tgt)
                {
                    return;
                }
            }
        }
    }
}
