//! Live tests of the database targets against PostgreSQL 16, MariaDB 11.8 and
//! MySQL 8.4 in the containers `r9sinks-pg` (127.0.0.1:25432),
//! `r9sinks-mariadb` (23337) and `r9sinks-mysql` (23336). Skipped unless
//! `SPRIG_SINKS_DB_LIVE=1`.
//!
//! ```sh
//! docker run -d --name r9sinks-pg --memory 768m -e POSTGRES_PASSWORD=sinkpw \
//!   -e POSTGRES_USER=sink -e POSTGRES_DB=sinkdb -p 127.0.0.1:25432:5432 postgres:16
//! docker run -d --name r9sinks-mariadb --memory 768m -e MARIADB_ROOT_PASSWORD=rootpw \
//!   -e MARIADB_DATABASE=sinkdb -e MARIADB_USER=sink -e MARIADB_PASSWORD=sinkpw \
//!   -p 127.0.0.1:23337:3306 mariadb:11.8
//! docker run -d --name r9sinks-mysql --memory 768m -e MYSQL_ROOT_PASSWORD=rootpw \
//!   -e MYSQL_DATABASE=sinkdb -e MYSQL_USER=sink -e MYSQL_PASSWORD=sinkpw \
//!   -p 127.0.0.1:23336:3306 mysql:8.4
//! SPRIG_SINKS_DB_LIVE=1 cargo test -p sprig-sinks --features wip-db --test db_live -- --test-threads 1 --nocapture
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::needless_pass_by_value,
    clippy::many_single_char_names,
    clippy::single_match_else
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use serde_json::{Map, Value, json};
use sprig_common::{ErrorKind, Result, SprigCode};
use sprig_sinks::model::{
    Batch, BatchOutcome, ChangeOp, ConnKind, ConnRef, ConnectionId, Guarantee, Outcome, Position,
    SinkDef, SinkId, SinkRecord, SinkState, SourceSpec, StartAt, TargetSpec, TaskEpoch, stream_key,
};
use sprig_sinks::policy::Destinations;
use sprig_sinks::runtime::{
    Cancel, Control, DeadInfo, DeadLetters, Directive, Event, PassAll, RunSettings, SourceReader,
    Target, TaskEnd, TaskInfo, TaskParts, run_task,
};
use sprig_sinks::target::{Conn, MemSecrets, Opts, TargetCtx};

fn live() -> bool {
    std::env::var("SPRIG_SINKS_DB_LIVE").is_ok_and(|v| v == "1")
}

#[derive(Clone, Copy, Debug)]
struct Server {
    name: &'static str,
    kind: ConnKind,
    port: u16,
    tls: &'static str,
}

const SERVERS: [Server; 3] = [
    Server {
        name: "postgresql-16",
        kind: ConnKind::Postgresql,
        port: 25432,
        tls: "off",
    },
    Server {
        name: "mariadb-11.8",
        kind: ConnKind::Mysql,
        port: 23337,
        tls: "off",
    },
    Server {
        name: "mysql-8.4",
        kind: ConnKind::Mysql,
        port: 23336,
        tls: "required",
    },
];

fn uniq(tag: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros()
        % 1_000_000_000;
    format!("r9s_{tag}_{n}")
}

fn conn(s: Server, port: u16) -> Conn {
    let mut options = Map::new();
    options.insert("host".into(), json!("127.0.0.1"));
    options.insert("port".into(), json!(port));
    options.insert("database".into(), json!("sinkdb"));
    options.insert("tls".into(), json!(s.tls));
    Conn {
        name: "dest".into(),
        kind: s.kind,
        options,
        secrets: Arc::new(MemSecrets::of(&[
            ("username", "sink"),
            ("password", "sinkpw"),
        ])),
        dest: Arc::new(Destinations::allow_list(&[
            "127.0.0.1:25432",
            "127.0.0.1:23336",
            "127.0.0.1:23337",
            "127.0.0.1:23999",
        ])),
    }
}

fn ctx(
    s: Server,
    table: &str,
    guarantee: Guarantee,
    source: SourceSpec,
    opts: &[(&str, &str)],
    epoch: u64,
) -> TargetCtx {
    ctx_port(s, s.port, table, guarantee, source, opts, epoch)
}

fn ctx_port(
    s: Server,
    port: u16,
    table: &str,
    guarantee: Guarantee,
    source: SourceSpec,
    opts: &[(&str, &str)],
    epoch: u64,
) -> TargetCtx {
    let sink = SinkDef {
        id: SinkId(77),
        db_id: 1,
        name: "to_db".into(),
        source,
        filter_sql: None,
        projection: None,
        transform: None,
        target: TargetSpec::Database {
            table: table.into(),
        },
        connections: vec![ConnRef::Tenant {
            id: ConnectionId(5),
            name: "dest".into(),
        }],
        guarantee,
        dead_letter: None,
        start_at: StartAt::Earliest,
        owner_user_id: 1,
        state: SinkState::Running,
        state_reason: None,
        definition_ver: 1,
        options: BTreeMap::new(),
    };
    TargetCtx {
        task: TaskInfo {
            tenant: 9,
            sink: 77,
            task_no: 0,
            epoch: TaskEpoch(epoch),
        },
        sink: Arc::new(sink),
        conn: Some(conn(s, port)),
        extra: Vec::new(),
        opts: Opts(
            opts.iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        ),
        db_name: "shop".into(),
    }
}

fn stream_src() -> SourceSpec {
    SourceSpec::Stream {
        id: 1,
        name: "events".into(),
        partitions: 3,
    }
}

fn srec(partition: u32, offset: u64, value: Value) -> SinkRecord {
    SinkRecord {
        key: value
            .get("id")
            .map(|id| json!({"id": id}).to_string().into_bytes()),
        value: Some(value.to_string().into_bytes()),
        headers: Vec::new(),
        op: None,
        before: None,
        after: None,
        ts_ms: 1_700_000_000_000,
        idem_key: stream_key(1, partition, offset, 0),
        pos: Position::Stream {
            stream: 1,
            partition,
            offset,
            epoch: 0,
        },
    }
}

fn crec(seq: u64, op: ChangeOp, before: Option<Value>, after: Option<Value>) -> SinkRecord {
    SinkRecord {
        key: None,
        value: None,
        headers: Vec::new(),
        op: Some(op),
        before,
        after,
        ts_ms: 1_700_000_000_000,
        idem_key: format!("c/1/0-1-{seq}/0"),
        pos: Position::Change {
            event: format!("0-1-{seq}"),
            token: String::new(),
            shard: 0,
            seq,
        },
    }
}

fn batch(records: Vec<SinkRecord>, eo: bool) -> Batch {
    let mut b = Batch::new(records, 0);
    if eo {
        b.ends = sprig_sinks::model::unit_ends(&b.records);
    }
    b
}

/// A direct client for setup and checks.
enum Raw {
    My(mysql_async::Pool),
    Pg(sqlx::PgPool),
}

impl Raw {
    async fn new(s: Server) -> Self {
        match s.kind {
            ConnKind::Postgresql => Raw::Pg(
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(2)
                    .connect(&format!(
                        "postgres://sink:sinkpw@127.0.0.1:{}/sinkdb?sslmode=disable",
                        s.port
                    ))
                    .await
                    .unwrap(),
            ),
            _ => {
                let mut b = mysql_async::OptsBuilder::default()
                    .ip_or_hostname("127.0.0.1")
                    .tcp_port(s.port)
                    .user(Some("sink"))
                    .pass(Some("sinkpw"))
                    .db_name(Some("sinkdb"))
                    .pool_opts(
                        mysql_async::PoolOpts::default()
                            .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
                            .with_inactive_connection_ttl(Duration::from_secs(30)),
                    );
                if s.tls == "required" {
                    b = b.ssl_opts(Some(
                        mysql_async::SslOpts::default()
                            .with_danger_accept_invalid_certs(true)
                            .with_danger_skip_domain_validation(true),
                    ));
                }
                Raw::My(mysql_async::Pool::new(b))
            }
        }
    }

    async fn exec(&self, sql: &str) {
        match self {
            Raw::My(p) => p.get_conn().await.unwrap().query_drop(sql).await.unwrap(),
            Raw::Pg(p) => {
                sqlx::raw_sql(sql).execute(p).await.unwrap();
            }
        }
    }

    /// Rows of `sql` as text cells.
    async fn rows(&self, sql: &str) -> Vec<Vec<Option<String>>> {
        match self {
            Raw::My(p) => {
                let rows: Vec<mysql_async::Row> =
                    p.get_conn().await.unwrap().query(sql).await.unwrap();
                rows.into_iter()
                    .map(|r| {
                        (0..r.len())
                            .map(|i| {
                                r.get_opt::<Option<String>, _>(i)
                                    .and_then(std::result::Result::ok)
                                    .flatten()
                            })
                            .collect()
                    })
                    .collect()
            }
            Raw::Pg(p) => {
                use sqlx::Row as _;
                let wrapped = format!("SELECT * FROM ({sql}) AS x");
                // Every column as text.
                let rows = sqlx::query(&wrapped).fetch_all(p).await.unwrap();
                rows.iter()
                    .map(|r| {
                        (0..r.len())
                            .map(|i| {
                                r.try_get::<Option<String>, _>(i)
                                    .ok()
                                    .flatten()
                                    .or_else(|| {
                                        r.try_get::<Option<i64>, _>(i)
                                            .ok()
                                            .flatten()
                                            .map(|n| n.to_string())
                                    })
                                    .or_else(|| {
                                        r.try_get::<Option<bool>, _>(i)
                                            .ok()
                                            .flatten()
                                            .map(|b| if b { "1" } else { "0" }.to_owned())
                                    })
                            })
                            .collect()
                    })
                    .collect()
            }
        }
    }

    async fn count(&self, table: &str) -> i64 {
        self.rows(&format!("SELECT CAST(COUNT(*) AS CHAR(20)) FROM {table}"))
            .await
            .first()
            .and_then(|r| r[0].clone())
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    async fn close(self) {
        match self {
            Raw::My(p) => {
                let _ = p.disconnect().await;
            }
            Raw::Pg(p) => p.close().await,
        }
    }
}

fn pg(s: Server) -> bool {
    s.kind == ConnKind::Postgresql
}

fn text_cast(s: Server) -> &'static str {
    if pg(s) { "TEXT" } else { "CHAR(200)" }
}

fn ver_type(s: Server) -> &'static str {
    if pg(s) { "BYTEA" } else { "VARBINARY(16)" }
}

fn is_code(e: &sprig_common::SprigError, c: SprigCode) -> bool {
    e.kind == ErrorKind::Sprig(c)
}

async fn open(c: &TargetCtx) -> Box<dyn Target> {
    let mut t = sprig_sinks::target::db::build(c).await.unwrap();
    t.open(&c.task).await.unwrap();
    t
}

#[tokio::test]
async fn upserts_deletes_and_appends() {
    if !live() {
        eprintln!("skipped: SPRIG_SINKS_DB_LIVE is not 1");
        return;
    }
    for s in SERVERS {
        let raw = Raw::new(s).await;
        let t = uniq("up");
        raw.exec(&format!(
            "CREATE TABLE {t} (id INT NOT NULL PRIMARY KEY, status VARCHAR(20), n INT, _sprig_ver {})",
            ver_type(s)
        ))
        .await;
        let c = ctx(s, &t, Guarantee::AtLeastOnce, stream_src(), &[], 1);
        let tg = open(&c).await;
        let b = batch(
            vec![
                srec(0, 10, json!({"id": 1, "status": "new", "n": 1})),
                srec(0, 11, json!({"id": 2, "status": "new", "n": 2})),
                srec(0, 12, json!({"id": 1, "status": "paid", "n": 3})),
            ],
            false,
        );
        assert_eq!(
            tg.deliver(&b).await.unwrap(),
            BatchOutcome::All(Outcome::Ok)
        );
        // A replay of an older version never moves the row back.
        let old = batch(
            vec![srec(0, 10, json!({"id": 1, "status": "new", "n": 1}))],
            false,
        );
        assert_eq!(
            tg.deliver(&old).await.unwrap(),
            BatchOutcome::All(Outcome::Ok)
        );
        let r = raw
            .rows(&format!(
                "SELECT CAST(status AS {}) FROM {t} WHERE id = 1",
                text_cast(s)
            ))
            .await;
        assert_eq!(r[0][0].as_deref(), Some("paid"), "{}", s.name);
        // A newer version does.
        let newer = batch(
            vec![srec(0, 20, json!({"id": 1, "status": "shipped", "n": 4}))],
            false,
        );
        tg.deliver(&newer).await.unwrap();
        let r = raw
            .rows(&format!(
                "SELECT CAST(status AS {}) FROM {t} WHERE id = 1",
                text_cast(s)
            ))
            .await;
        assert_eq!(r[0][0].as_deref(), Some("shipped"), "{}", s.name);
        // A tombstone (null value) older than the row leaves it; a newer one
        // deletes it.
        let mut tomb = srec(0, 15, json!({"id": 1}));
        tomb.value = None;
        tg.deliver(&batch(vec![tomb.clone()], false)).await.unwrap();
        assert_eq!(raw.count(&t).await, 2, "{}", s.name);
        tomb.pos = Position::Stream {
            stream: 1,
            partition: 0,
            offset: 21,
            epoch: 0,
        };
        tg.deliver(&batch(vec![tomb], false)).await.unwrap();
        assert_eq!(raw.count(&t).await, 1, "{}", s.name);
        drop(tg);

        // Change records: soft deletes and key changes.
        let t2 = uniq("soft");
        raw.exec(&format!(
            "CREATE TABLE {t2} (id INT NOT NULL PRIMARY KEY, status VARCHAR(20), \
             _sprig_deleted BOOLEAN NOT NULL DEFAULT FALSE, _sprig_ver {})",
            ver_type(s)
        ))
        .await;
        let c = ctx(
            s,
            &t2,
            Guarantee::AtLeastOnce,
            SourceSpec::Changes {
                objects: vec![(1, "orders".into())],
            },
            &[("delete_mode", "soft")],
            1,
        );
        let tg = open(&c).await;
        tg.deliver(&batch(
            vec![
                crec(
                    1,
                    ChangeOp::Create,
                    None,
                    Some(json!({"id": 1, "status": "a"})),
                ),
                crec(
                    2,
                    ChangeOp::Create,
                    None,
                    Some(json!({"id": 2, "status": "b"})),
                ),
                crec(
                    3,
                    ChangeOp::Delete,
                    Some(json!({"id": 1, "status": "a"})),
                    None,
                ),
                crec(
                    4,
                    ChangeOp::Update,
                    Some(json!({"id": 2, "status": "b"})),
                    Some(json!({"id": 3, "status": "b"})),
                ),
            ],
            false,
        ))
        .await
        .unwrap();
        let rows = raw
            .rows(&format!(
                "SELECT CAST(id AS CHAR(10)), CASE WHEN _sprig_deleted THEN '1' ELSE '0' END \
                 FROM {t2} ORDER BY id"
            ))
            .await;
        let got: Vec<(String, String)> = rows
            .into_iter()
            .map(|r| {
                (
                    r[0].clone().unwrap().trim().to_owned(),
                    r[1].clone().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("1".into(), "1".into()),
                ("2".into(), "1".into()),
                ("3".into(), "0".into())
            ],
            "{}",
            s.name
        );
        // An older upsert can't revive a soft-deleted row.
        tg.deliver(&batch(
            vec![crec(
                2,
                ChangeOp::Create,
                None,
                Some(json!({"id": 1, "status": "a"})),
            )],
            false,
        ))
        .await
        .unwrap();
        let r = raw
            .rows(&format!(
                "SELECT CASE WHEN _sprig_deleted THEN '1' ELSE '0' END FROM {t2} WHERE id = 1"
            ))
            .await;
        assert_eq!(r[0][0].as_deref(), Some("1"), "{}", s.name);
        drop(tg);

        // Hard deletes and ignored deletes.
        for (mode, left) in [("delete", 0), ("ignore", 1)] {
            let t3 = uniq("del");
            raw.exec(&format!(
                "CREATE TABLE {t3} (id INT NOT NULL PRIMARY KEY, status VARCHAR(20), _sprig_ver {})",
                ver_type(s)
            ))
            .await;
            let c = ctx(
                s,
                &t3,
                Guarantee::AtLeastOnce,
                SourceSpec::Changes {
                    objects: vec![(1, "orders".into())],
                },
                &[("delete_mode", mode)],
                1,
            );
            let tg = open(&c).await;
            tg.deliver(&batch(
                vec![
                    crec(
                        1,
                        ChangeOp::Create,
                        None,
                        Some(json!({"id": 1, "status": "a"})),
                    ),
                    crec(
                        2,
                        ChangeOp::Delete,
                        Some(json!({"id": 1, "status": "a"})),
                        None,
                    ),
                ],
                false,
            ))
            .await
            .unwrap();
            assert_eq!(raw.count(&t3).await, left, "{} {mode}", s.name);
            raw.exec(&format!("DROP TABLE {t3}")).await;
        }

        // Append drops duplicates by _sprig_idem.
        let t4 = uniq("app");
        let c = ctx(
            s,
            &t4,
            Guarantee::AtLeastOnce,
            stream_src(),
            &[("mode", "append"), ("schema_policy", "create")],
            1,
        );
        let tg = open(&c).await;
        let b = batch(
            vec![
                srec(0, 1, json!({"id": 1, "v": "x"})),
                srec(0, 2, json!({"id": 1, "v": "y"})),
            ],
            false,
        );
        tg.deliver(&b).await.unwrap();
        tg.deliver(&b).await.unwrap();
        assert_eq!(raw.count(&t4).await, 2, "{}", s.name);
        drop(tg);
        for x in [&t, &t2, &t4] {
            raw.exec(&format!("DROP TABLE {x}")).await;
        }
        raw.close().await;
        eprintln!("{}: upserts, deletes and appends ok", s.name);
    }
}

#[tokio::test]
async fn schema_policies() {
    if !live() {
        return;
    }
    for s in SERVERS {
        let raw = Raw::new(s).await;
        let t = uniq("sch");
        // strict: no table.
        let c = ctx(s, &t, Guarantee::AtLeastOnce, stream_src(), &[], 1);
        let tg = open(&c).await;
        let b = batch(vec![srec(0, 1, json!({"id": 1, "name": "a"}))], false);
        let e = tg.deliver(&b).await.unwrap_err();
        assert!(is_code(&e, SprigCode::SinkSchemaMismatch), "{} {e}", s.name);
        drop(tg);
        // create: makes it, keyed by the record's key.
        let c = ctx(
            s,
            &t,
            Guarantee::AtLeastOnce,
            stream_src(),
            &[("schema_policy", "create")],
            1,
        );
        let tg = open(&c).await;
        tg.deliver(&b).await.unwrap();
        assert_eq!(raw.count(&t).await, 1);
        drop(tg);
        // strict: a new column refused; add_columns adds it.
        let c = ctx(s, &t, Guarantee::AtLeastOnce, stream_src(), &[], 1);
        let tg = open(&c).await;
        let b2 = batch(
            vec![srec(0, 2, json!({"id": 2, "name": "b", "extra": 7}))],
            false,
        );
        let e = tg.deliver(&b2).await.unwrap_err();
        assert!(is_code(&e, SprigCode::SinkSchemaMismatch), "{} {e}", s.name);
        drop(tg);
        let c = ctx(
            s,
            &t,
            Guarantee::AtLeastOnce,
            stream_src(),
            &[("schema_policy", "add_columns")],
            1,
        );
        let tg = open(&c).await;
        tg.deliver(&b2).await.unwrap();
        let r = raw
            .rows(&format!(
                "SELECT CAST(extra AS CHAR(10)) FROM {t} WHERE id = 2"
            ))
            .await;
        assert_eq!(r[0][0].as_deref().map(str::trim), Some("7"), "{}", s.name);
        drop(tg);
        raw.exec(&format!("DROP TABLE {t}")).await;
        raw.close().await;
        eprintln!("{}: schema policies ok", s.name);
    }
}

#[tokio::test]
async fn exactly_once_skips_resent_batches_and_fences_stale_tasks() {
    if !live() {
        return;
    }
    for s in SERVERS {
        let raw = Raw::new(s).await;
        let t = uniq("eo");
        // Append rows with no unique key: a record applied twice shows.
        raw.exec(&format!(
            "CREATE TABLE {t} (id INT, v INT, _sprig_op VARCHAR(1), _sprig_ts BIGINT, _sprig_idem VARCHAR(200))"
        ))
        .await;
        let o = [("mode", "append")];
        let c1 = ctx(s, &t, Guarantee::ExactlyOnce, stream_src(), &o, 1);
        let mut t1 = sprig_sinks::target::db::build(&c1).await.unwrap();
        assert!(t1.exactly_once());
        assert!(t1.open(&c1.task).await.unwrap().is_empty());
        let recs: Vec<SinkRecord> = (0..10)
            .map(|i| srec(i % 2, u64::from(i / 2), json!({"id": i, "v": i})))
            .collect();
        let b = batch(recs[..6].to_vec(), true);
        t1.deliver(&b).await.unwrap();
        // The same batch again (its answer was lost), then an overlapping one.
        t1.deliver(&b).await.unwrap();
        t1.deliver(&batch(recs.clone(), true)).await.unwrap();
        assert_eq!(raw.count(&t).await, 10, "{}", s.name);
        // A new task (epoch 2) reads the positions and fences epoch 1.
        let c2 = ctx(s, &t, Guarantee::ExactlyOnce, stream_src(), &o, 2);
        let mut t2 = sprig_sinks::target::db::build(&c2).await.unwrap();
        let mut pos = t2.open(&c2.task).await.unwrap();
        pos.sort_by_key(|p| p.pos.unit_seq());
        let got: Vec<_> = pos.iter().map(|p| p.pos.unit_seq().unwrap()).collect();
        assert_eq!(got, vec![(0, 4), (1, 4)], "{}", s.name);
        let more = batch(vec![srec(0, 5, json!({"id": 10, "v": 10}))], true);
        let e = t1.deliver(&more).await.unwrap_err();
        assert!(is_code(&e, SprigCode::SinkFenced), "{} {e}", s.name);
        t2.deliver(&more).await.unwrap();
        assert_eq!(raw.count(&t).await, 11, "{}", s.name);
        // A task older than the destination's epoch can't even open.
        let mut t0 = sprig_sinks::target::db::build(&c1).await.unwrap();
        let e = t0.open(&c1.task).await.unwrap_err();
        assert!(is_code(&e, SprigCode::SinkFenced), "{} {e}", s.name);
        drop((t0, t1, t2));
        raw.exec(&format!("DROP TABLE {t}")).await;
        raw.exec("DELETE FROM __sprig_sink_offsets WHERE sink_key LIKE '9/77/%'")
            .await;

        // Queue sources: seen keys in the same transaction.
        let tq = uniq("eoq");
        raw.exec(&format!(
            "CREATE TABLE {tq} (id INT, _sprig_op VARCHAR(1), _sprig_ts BIGINT, _sprig_idem VARCHAR(200))"
        ))
        .await;
        let cq = ctx(
            s,
            &tq,
            Guarantee::ExactlyOnce,
            SourceSpec::Queue {
                id: 4,
                name: "jobs".into(),
            },
            &o,
            1,
        );
        let mut tgq = sprig_sinks::target::db::build(&cq).await.unwrap();
        tgq.open(&cq.task).await.unwrap();
        let q = |id: u64| SinkRecord {
            key: None,
            value: Some(json!({"id": id}).to_string().into_bytes()),
            headers: Vec::new(),
            op: None,
            before: None,
            after: None,
            ts_ms: 1,
            idem_key: format!("q/4/m{id}"),
            pos: Position::Queue {
                queue: 4,
                msg_id: format!("m{id}"),
                delivery: String::new(),
            },
        };
        let b = batch(vec![q(1), q(2)], true);
        tgq.deliver(&b).await.unwrap();
        tgq.deliver(&batch(vec![q(2), q(3)], true)).await.unwrap();
        assert_eq!(raw.count(&tq).await, 3, "{}", s.name);
        drop(tgq);
        raw.exec(&format!("DROP TABLE {tq}")).await;
        raw.exec("DELETE FROM __sprig_sink_seen WHERE sink_key = '9/77'")
            .await;
        raw.close().await;
        eprintln!("{}: exactly once ok", s.name);
    }
}

// ---- the runtime against real servers ----

struct Stream {
    parts: Vec<Vec<SinkRecord>>,
    committed: Mutex<HashMap<u32, u64>>,
}

struct Reader {
    s: Arc<Stream>,
    at: HashMap<u32, u64>,
    turn: u32,
}

impl Reader {
    fn new(s: Arc<Stream>) -> Self {
        let at = s.committed.lock().unwrap().clone();
        Self { s, at, turn: 0 }
    }
}

#[async_trait]
impl SourceReader for Reader {
    async fn next_batch(
        &mut self,
        max_records: usize,
        _max_bytes: u64,
        wait: Duration,
    ) -> Result<Vec<SinkRecord>> {
        let n = u32::try_from(self.s.parts.len()).unwrap();
        let mut out = Vec::new();
        for _ in 0..n {
            let p = self.turn % n;
            self.turn += 1;
            let at = self.at.entry(p).or_insert(0);
            let part = &self.s.parts[p as usize];
            while out.len() < max_records && usize::try_from(*at).unwrap() < part.len() {
                out.push(part[usize::try_from(*at).unwrap()].clone());
                *at += 1;
            }
        }
        if out.is_empty() {
            tokio::time::sleep(wait).await;
        }
        Ok(out)
    }

    async fn settle(&mut self, done: &[Position]) -> Result<()> {
        let mut c = self.s.committed.lock().unwrap();
        for p in done {
            if let Position::Stream {
                partition, offset, ..
            } = p
            {
                let e = c.entry(*partition).or_insert(0);
                *e = (*e).max(offset + 1);
            }
        }
        Ok(())
    }

    async fn seek_after(&mut self, pos: &Position) -> Result<()> {
        if let Position::Stream {
            partition, offset, ..
        } = pos
        {
            self.at.insert(*partition, offset + 1);
        }
        Ok(())
    }
}

#[derive(Default)]
struct Dead(Mutex<Vec<(String, String)>>);

#[async_trait]
impl DeadLetters for Dead {
    async fn write(&self, rec: &SinkRecord, info: &DeadInfo) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .push((rec.idem_key.clone(), info.reason.clone()));
        Ok(())
    }
}

#[derive(Default)]
struct Ctl(Mutex<Vec<Event>>);

#[async_trait]
impl Control for Ctl {
    async fn poll(&self) -> Result<Directive> {
        Ok(Directive::Run)
    }
    async fn report(&self, ev: Event) -> Result<()> {
        if !matches!(ev, Event::Delivered(_)) {
            self.0.lock().unwrap().push(ev);
        }
        Ok(())
    }
    async fn check_epoch(&self) -> Result<()> {
        Ok(())
    }
}

fn settings() -> RunSettings {
    RunSettings {
        lanes: 4,
        batch_max_records: 100,
        batch_max_wait: Duration::from_millis(20),
        retry_initial: Duration::from_millis(100),
        retry_max: Duration::from_millis(2000),
        delivery_timeout: Duration::from_secs(10),
        ..RunSettings::default()
    }
}

struct RunOut {
    kills: u32,
    elapsed: Duration,
    dead: Vec<(String, String)>,
}

/// Runs tasks over `stream` until every partition is committed; kills
/// (aborts) a task at random moments `kills` times; `during` runs once
/// after the first second (a server restart).
async fn run_until_done(
    stream: &Arc<Stream>,
    make: impl Fn(u64) -> TargetCtx,
    kills: u32,
    during: Option<&'static str>,
) -> RunOut {
    let dead = Arc::new(Dead::default());
    let ctl = Arc::new(Ctl::default());
    let total: HashMap<u32, u64> = stream
        .parts
        .iter()
        .enumerate()
        .map(|(p, v)| (u32::try_from(p).unwrap(), v.len() as u64))
        .collect();
    let start = Instant::now();
    let mut killed = 0;
    let mut life = 0u64;
    let mut restarted = during.is_none();
    loop {
        life += 1;
        let c = make(life);
        let target = loop {
            match sprig_sinks::target::db::build(&c).await {
                Ok(t) => break t,
                Err(e) => {
                    eprintln!("build: {e}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        };
        let parts = TaskParts {
            info: c.task.clone(),
            source: Box::new(Reader::new(Arc::clone(stream))),
            target,
            dead: Arc::clone(&dead) as Arc<dyn DeadLetters>,
            control: Arc::clone(&ctl) as Arc<dyn Control>,
            prepare: Arc::new(PassAll),
            settings: settings(),
            seed: Some(life),
            reject_via_source: false,
        };
        let cancel = Cancel::new();
        let mut h = tokio::spawn(run_task(parts, cancel.clone()));
        let kill_at = Instant::now() + Duration::from_millis(300 + (life * 7919 % 900));
        let finished = loop {
            let done = {
                let c = stream.committed.lock().unwrap();
                total
                    .iter()
                    .all(|(p, n)| c.get(p).copied().unwrap_or(0) >= *n)
            };
            if done {
                cancel.cancel();
                let r = (&mut h).await.unwrap().unwrap();
                assert_eq!(r, TaskEnd::Cancelled);
                break true;
            }
            if !restarted && start.elapsed() > Duration::from_secs(1) {
                restarted = true;
                let name = during.unwrap();
                let out = std::process::Command::new("docker")
                    .args(["restart", "-t", "2", name])
                    .output()
                    .unwrap();
                assert!(out.status.success());
                eprintln!("restarted {name} mid-run");
            }
            if killed < kills && Instant::now() >= kill_at {
                h.abort();
                let _ = (&mut h).await;
                killed += 1;
                break false;
            }
            assert!(start.elapsed() < Duration::from_secs(300), "no progress");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        if finished {
            break;
        }
    }
    RunOut {
        kills: killed,
        elapsed: start.elapsed(),
        dead: dead.0.lock().unwrap().clone(),
    }
}

fn make_stream(n: u64, parts: u32, poison: &HashSet<u64>) -> Arc<Stream> {
    let mut v = vec![Vec::new(); parts as usize];
    for i in 0..n {
        let p = u32::try_from(i % u64::from(parts)).unwrap();
        let off = v[p as usize].len() as u64;
        // A poison row: `v` too long for its column.
        let val = if poison.contains(&i) {
            "x".repeat(40)
        } else {
            format!("v{i}")
        };
        v[p as usize].push(srec(p, off, json!({"id": i, "v": val})));
    }
    Arc::new(Stream {
        parts: v,
        committed: Mutex::new(HashMap::new()),
    })
}

#[tokio::test]
async fn exactly_once_through_the_runtime_with_kills() {
    if !live() {
        return;
    }
    for s in SERVERS {
        let raw = Raw::new(s).await;
        let t = uniq("rt");
        raw.exec(&format!(
            "CREATE TABLE {t} (id INT, v VARCHAR(20), _sprig_op VARCHAR(1), _sprig_ts BIGINT, _sprig_idem VARCHAR(200))"
        ))
        .await;
        let n = 3000;
        let stream = make_stream(n, 3, &HashSet::new());
        let out = run_until_done(
            &stream,
            |life| {
                ctx(
                    s,
                    &t,
                    Guarantee::ExactlyOnce,
                    stream_src(),
                    &[("mode", "append")],
                    life,
                )
            },
            8,
            None,
        )
        .await;
        let rows = raw.count(&t).await;
        let distinct = raw
            .rows(&format!(
                "SELECT CAST(COUNT(DISTINCT _sprig_idem) AS CHAR(20)) FROM {t}"
            ))
            .await[0][0]
            .clone()
            .unwrap();
        assert_eq!(rows, i64::try_from(n).unwrap(), "{}: rows", s.name);
        assert_eq!(distinct.trim(), n.to_string(), "{}", s.name);
        eprintln!(
            "{}: exactly once through run_task: {n} records, {} kills, each applied once, {:.0} records/s",
            s.name,
            out.kills,
            n as f64 / out.elapsed.as_secs_f64()
        );
        raw.exec(&format!("DROP TABLE {t}")).await;
        raw.exec("DELETE FROM __sprig_sink_offsets WHERE sink_key LIKE '9/77/%'")
            .await;
        raw.close().await;
    }
}

#[tokio::test]
async fn poison_rows_are_split_out_and_dead_lettered() {
    if !live() {
        return;
    }
    for s in SERVERS {
        let raw = Raw::new(s).await;
        let t = uniq("poi");
        raw.exec(&format!(
            "CREATE TABLE {t} (id INT NOT NULL PRIMARY KEY, v VARCHAR(20), _sprig_ver {})",
            ver_type(s)
        ))
        .await;
        let poison: HashSet<u64> = [17, 250, 999].into();
        let n = 1000;
        let stream = make_stream(n, 3, &poison);
        let out = run_until_done(
            &stream,
            |life| ctx(s, &t, Guarantee::AtLeastOnce, stream_src(), &[], life),
            0,
            None,
        )
        .await;
        assert_eq!(
            raw.count(&t).await,
            i64::try_from(n).unwrap() - 3,
            "{}",
            s.name
        );
        assert_eq!(out.dead.len(), 3, "{}: {:?}", s.name, out.dead);
        eprintln!(
            "{}: {n} records, 3 poison rows dead-lettered ({}), {:.0} records/s",
            s.name,
            out.dead[0].1,
            n as f64 / out.elapsed.as_secs_f64()
        );
        raw.exec(&format!("DROP TABLE {t}")).await;
        raw.close().await;
    }
}

#[tokio::test]
async fn a_down_destination_retries_then_recovers() {
    if !live() {
        return;
    }
    // Nothing listens on 23999: every delivery is a destination-wide retry.
    for s in SERVERS {
        let c = ctx_port(s, 23999, "t", Guarantee::AtLeastOnce, stream_src(), &[], 1);
        let tg = sprig_sinks::target::db::build(&c).await.unwrap();
        let e = tg
            .deliver(&batch(vec![srec(0, 1, json!({"id": 1}))], false))
            .await
            .unwrap_err();
        assert!(is_code(&e, SprigCode::SinkDeliveryFailed), "{} {e}", s.name);
        assert!(e.retryable);
    }
    // PostgreSQL and MariaDB restarted mid-run, under exactly once.
    for (s, name) in [(SERVERS[0], "r9sinks-pg"), (SERVERS[1], "r9sinks-mariadb")] {
        let raw = Raw::new(s).await;
        let t = uniq("rs");
        raw.exec(&format!(
            "CREATE TABLE {t} (id INT, v VARCHAR(20), _sprig_op VARCHAR(1), _sprig_ts BIGINT, _sprig_idem VARCHAR(200))"
        ))
        .await;
        raw.close().await;
        let n = 3000;
        let stream = make_stream(n, 3, &HashSet::new());
        let out = run_until_done(
            &stream,
            |life| {
                ctx(
                    s,
                    &t,
                    Guarantee::ExactlyOnce,
                    stream_src(),
                    &[("mode", "append")],
                    life,
                )
            },
            2,
            Some(name),
        )
        .await;
        // Wait for the server to answer again.
        let raw = loop {
            if let Ok(r) = tokio::time::timeout(Duration::from_secs(5), Raw::new(s)).await {
                break r;
            }
        };
        assert_eq!(raw.count(&t).await, i64::try_from(n).unwrap(), "{}", s.name);
        eprintln!(
            "{}: restarted mid-run: {n} records each applied once, {} kills, {:.1} s",
            s.name,
            out.kills,
            out.elapsed.as_secs_f64()
        );
        raw.exec(&format!("DROP TABLE {t}")).await;
        raw.exec("DELETE FROM __sprig_sink_offsets WHERE sink_key LIKE '9/77/%'")
            .await;
        raw.close().await;
    }
}

#[tokio::test]
async fn throughput_without_faults() {
    if !live() {
        return;
    }
    for s in SERVERS {
        for (g, mode) in [
            (Guarantee::AtLeastOnce, "upsert"),
            (Guarantee::ExactlyOnce, "upsert"),
        ] {
            let raw = Raw::new(s).await;
            let t = uniq("tp");
            raw.exec(&format!(
                "CREATE TABLE {t} (id INT NOT NULL PRIMARY KEY, v VARCHAR(20), _sprig_ver {})",
                ver_type(s)
            ))
            .await;
            let n = 20_000;
            let stream = make_stream(n, 4, &HashSet::new());
            let out = run_until_done(
                &stream,
                |life| ctx(s, &t, g, stream_src(), &[("mode", mode)], life),
                0,
                None,
            )
            .await;
            assert_eq!(raw.count(&t).await, i64::try_from(n).unwrap());
            eprintln!(
                "{}: {g:?} {mode}: {n} records in {:.1} s, {:.0} records/s",
                s.name,
                out.elapsed.as_secs_f64(),
                n as f64 / out.elapsed.as_secs_f64()
            );
            raw.exec(&format!("DROP TABLE {t}")).await;
            raw.exec("DELETE FROM __sprig_sink_offsets WHERE sink_key LIKE '9/77/%'")
                .await;
            raw.close().await;
        }
    }
}
