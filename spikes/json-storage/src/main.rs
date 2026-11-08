//! Throwaway spike (month-1, "JSON storage against MongoDB"): a minimal MongoDB wire
//! server that stores collections in MariaDB the way chapter 31 describes.
//! Not production code; see sprigdb-plan/decisions/spike-json-storage.md.

mod fastconv;
mod json;
mod keyenc;
mod translate;
mod wire;

use anyhow::{anyhow, bail, Context, Result};
use bson::raw::{RawArrayBuf, RawBsonRef, RawDocument, RawDocumentBuf};
use bson::{rawdoc, DateTime};
use mysql_async::prelude::*;
use mysql_async::{Opts, OptsBuilder, Params, Pool, PoolConstraints, PoolOpts, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use translate::{Class, FieldCol};

type Row = (Vec<u8>, Option<Vec<u8>>);

struct Coll {
    schema: String,
    table: String,
    fields: RwLock<Vec<FieldCol>>,
}

struct Cursor {
    ns: String,
    rows: VecDeque<Row>,
    proj: Option<RawDocumentBuf>,
}

#[derive(Default)]
struct OpStat {
    n: AtomicU64,
    total: AtomicU64,
    sql: AtomicU64,
    pool: AtomicU64,
    conv: AtomicU64,
}

const OPS: [&str; 6] = ["insert", "find", "update", "delete", "getMore", "other"];

struct App {
    pool: Pool,
    colls: Mutex<HashMap<String, Arc<Coll>>>,
    cursors: Mutex<HashMap<i64, Cursor>>,
    next_cursor: AtomicI64,
    next_conn: AtomicI64,
    hlc: AtomicU64,
    stats: [OpStat; 6],
}

#[derive(Default)]
struct Timing {
    sql: u64,
    pool: u64,
    conv: u64,
}

fn ns(t: Instant) -> u64 {
    t.elapsed().as_nanos() as u64
}

impl App {
    fn stamp(&self) -> u64 {
        let now = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64) << 16;
        let mut cur = self.hlc.load(Relaxed);
        loop {
            let next = now.max(cur + 1);
            match self.hlc.compare_exchange_weak(cur, next, Relaxed, Relaxed) {
                Ok(_) => return next,
                Err(c) => cur = c,
            }
        }
    }

    async fn conn(&self, tm: &mut Timing) -> Result<mysql_async::Conn> {
        let t = Instant::now();
        let c = self.pool.get_conn().await?;
        tm.pool += ns(t);
        Ok(c)
    }

    async fn coll(&self, db: &str, name: &str, tm: &mut Timing) -> Result<Arc<Coll>> {
        let key = format!("{db}.{name}");
        if let Some(c) = self.colls.lock().unwrap().get(&key) {
            return Ok(c.clone());
        }
        let ok = |s: &str| !s.is_empty() && s.len() <= 40 && s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_');
        if !ok(db) || !ok(name) {
            bail!("spike: unsupported namespace {key}");
        }
        let mut conn = self.conn(tm).await?;
        conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{db}`")).await?;
        conn.query_drop(format!(
            "CREATE TABLE IF NOT EXISTS `{db}`.`{name}` (
               _id VARBINARY(1024) NOT NULL,
               _t VARCHAR(255) NULL,
               doc JSON NOT NULL,
               _types JSON NULL,
               _v BIGINT UNSIGNED NOT NULL,
               PRIMARY KEY (_id)
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        ))
        .await?;
        let cols: Vec<(String, String)> = conn
            .exec(
                "SELECT COLUMN_NAME, COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_COMMENT LIKE 'sprig:%'",
                (db, name),
            )
            .await?;
        let fields = cols
            .into_iter()
            .filter_map(|(col, comment)| {
                let mut it = comment.splitn(3, ':');
                it.next();
                let class = Class::parse(it.next()?)?;
                Some(FieldCol { path: it.next()?.to_string(), class, col })
            })
            .collect();
        let c = Arc::new(Coll { schema: db.into(), table: name.into(), fields: RwLock::new(fields) });
        self.colls.lock().unwrap().insert(key, c.clone());
        Ok(c)
    }
}

fn get_i64(d: &RawDocument, k: &str) -> Result<Option<i64>> {
    Ok(match d.get(k)? {
        Some(RawBsonRef::Int32(i)) => Some(i as i64),
        Some(RawBsonRef::Int64(i)) => Some(i),
        Some(RawBsonRef::Double(f)) => Some(f as i64),
        _ => None,
    })
}

fn get_doc<'a>(d: &'a RawDocument, k: &str) -> Result<Option<&'a RawDocument>> {
    Ok(match d.get(k)? {
        Some(RawBsonRef::Document(x)) => Some(x),
        _ => None,
    })
}

fn get_str<'a>(d: &'a RawDocument, k: &str) -> Result<&'a str> {
    match d.get(k)? {
        Some(RawBsonRef::String(s)) => Ok(s),
        _ => Err(anyhow!("missing string field {k}")),
    }
}

/// Documents from an OP_MSG kind-1 sequence or from the body's array.
fn docs<'a>(body: &'a RawDocument, seqs: &'a [(String, Vec<RawDocumentBuf>)], name: &str) -> Result<Vec<&'a RawDocument>> {
    if let Some((_, v)) = seqs.iter().find(|(n, _)| n == name) {
        return Ok(v.iter().map(|d| d.as_ref()).collect());
    }
    let mut out = vec![];
    if let Some(RawBsonRef::Array(a)) = body.get(name)? {
        for x in a {
            match x? {
                RawBsonRef::Document(d) => out.push(d),
                _ => bail!("{name} element is not a document"),
            }
        }
    }
    Ok(out)
}

fn hello(conn_id: i64, legacy: bool, hello_ok: bool) -> RawDocumentBuf {
    let mut d = RawDocumentBuf::new();
    d.append(if legacy { "ismaster" } else { "isWritablePrimary" }, true);
    if hello_ok {
        d.append("helloOk", true);
    }
    d.append("msg", "isdbgrid");
    d.append("maxBsonObjectSize", 16_777_216i32);
    d.append("maxMessageSizeBytes", 48_000_000i32);
    d.append("maxWriteBatchSize", 100_000i32);
    d.append("localTime", DateTime::now());
    d.append("logicalSessionTimeoutMinutes", 30i32);
    d.append("connectionId", conn_id);
    d.append("minWireVersion", 0i32);
    d.append("maxWireVersion", 25i32);
    d.append("readOnly", false);
    d.append("ok", 1.0);
    d
}

fn project(d: RawDocumentBuf, proj: Option<&RawDocument>) -> Result<RawDocumentBuf> {
    let Some(p) = proj else { return Ok(d) };
    let mut incl = vec![];
    let mut excl = vec![];
    let mut id = true;
    for e in p {
        let (k, v) = e?;
        let on = !matches!(v, RawBsonRef::Int32(0) | RawBsonRef::Int64(0) | RawBsonRef::Boolean(false))
            && !matches!(v, RawBsonRef::Double(f) if f == 0.0);
        if k == "_id" {
            id = on;
        } else if on {
            incl.push(k);
        } else {
            excl.push(k);
        }
    }
    if incl.is_empty() && excl.is_empty() && id {
        return Ok(d);
    }
    let mut out = RawDocumentBuf::new();
    for e in d.iter() {
        let (k, v) = e?;
        let keep = if k == "_id" { id } else if !incl.is_empty() { incl.contains(&k) } else { !excl.contains(&k) };
        if keep {
            out.append_ref(k, v);
        }
    }
    Ok(out)
}

fn batch(rows: &mut VecDeque<Row>, n: usize, proj: Option<&RawDocument>, tm: &mut Timing) -> Result<RawArrayBuf> {
    let t = Instant::now();
    let mut arr = RawArrayBuf::new();
    let mut bytes = 0usize;
    for _ in 0..n {
        let Some((doc, types)) = rows.pop_front() else { break };
        let d = fastconv::to_bson_fast(&doc, types.as_deref()).or_else(|_| json::to_bson(&doc, types.as_deref()))?;
        let d = project(d, proj)?;
        bytes += d.as_bytes().len();
        arr.push(d);
        if bytes > 16 * 1024 * 1024 {
            break;
        }
    }
    tm.conv += ns(t);
    Ok(arr)
}

fn server_code(e: &anyhow::Error) -> Option<u16> {
    match e.downcast_ref::<mysql_async::Error>() {
        Some(mysql_async::Error::Server(s)) => Some(s.code),
        _ => None,
    }
}

fn parse_info(info: &str) -> (i64, i64) {
    // "Rows matched: 1  Changed: 1  Warnings: 0"
    let num = |key: &str| {
        info.find(key).and_then(|i| info[i + key.len()..].split_whitespace().next()).and_then(|s| s.parse().ok()).unwrap_or(0)
    };
    (num("Rows matched:"), num("Changed:"))
}

async fn run_cmd(app: &App, conn_id: i64, body: &RawDocument, seqs: &[(String, Vec<RawDocumentBuf>)], tm: &mut Timing) -> Result<(usize, RawDocumentBuf)> {
    let (name, first) = body.iter().next().context("empty command")??;
    let db = get_str(body, "$db").unwrap_or("test");
    match name {
        "hello" | "isMaster" | "ismaster" => {
            let hello_ok = matches!(body.get("helloOk")?, Some(RawBsonRef::Boolean(true)));
            Ok((5, hello(conn_id, name != "hello", hello_ok)))
        }
        "buildInfo" | "buildinfo" => Ok((5, rawdoc! {"version": "8.0.0", "versionArray": [8i32, 0i32, 0i32, 0i32], "maxBsonObjectSize": 16_777_216i32, "ok": 1.0})),
        "ping" | "endSessions" | "getLastError" | "getlasterror" => Ok((5, rawdoc! {"ok": 1.0})),
        "insert" => {
            let RawBsonRef::String(cname) = first else { bail!("insert: collection name") };
            let coll = app.coll(db, cname, tm).await?;
            let ds = docs(body, seqs, "documents")?;
            let t = Instant::now();
            let mut params: Vec<Value> = Vec::with_capacity(ds.len() * 4);
            for d in &ds {
                let s = json::to_stored(d)?;
                params.push(Value::Bytes(s.id_key));
                params.push(Value::Bytes(s.doc.into_bytes()));
                params.push(s.types.map(|t| Value::Bytes(t.into_bytes())).unwrap_or(Value::NULL));
                params.push(Value::UInt(app.stamp()));
            }
            tm.conv += ns(t);
            let rows = vec!["(?,?,?,?)"; ds.len()].join(",");
            let sql = format!("INSERT INTO `{}`.`{}` (_id, doc, _types, _v) VALUES {rows}", coll.schema, coll.table);
            let mut conn = app.conn(tm).await?;
            let t = Instant::now();
            let r = conn.exec_drop(log_sql(sql), Params::Positional(params)).await;
            tm.sql += ns(t);
            match r {
                Ok(()) => Ok((0, rawdoc! {"n": ds.len() as i32, "ok": 1.0})),
                Err(e) => {
                    let e = anyhow::Error::from(e);
                    if server_code(&e) == Some(1062) {
                        Ok((0, rawdoc! {"n": 0i32, "writeErrors": [{"index": 0i32, "code": 11000i32, "errmsg": format!("E11000 duplicate key error: {e}")}], "ok": 1.0}))
                    } else {
                        Err(e)
                    }
                }
            }
        }
        "find" => {
            let RawBsonRef::String(cname) = first else { bail!("find: collection name") };
            let coll = app.coll(db, cname, tm).await?;
            let fields = coll.fields.read().unwrap().clone();
            let w = translate::filter(get_doc(body, "filter")?, &fields)?;
            let order = translate::sort(get_doc(body, "sort")?, &fields)?;
            let mut limit = get_i64(body, "limit")?.unwrap_or(0);
            let mut single = matches!(body.get("singleBatch")?, Some(RawBsonRef::Boolean(true)));
            if limit < 0 {
                limit = -limit;
                single = true;
            }
            let skip = get_i64(body, "skip")?.unwrap_or(0);
            let bsize = get_i64(body, "batchSize")?.unwrap_or(101).max(1) as usize;
            // spike: an unlimited find is capped at 100k rows, buffered in the cursor
            let lim = if limit > 0 { limit } else { 100_000 };
            let sql = format!(
                "SELECT doc, _types FROM `{}`.`{}` WHERE {}{order} LIMIT {lim} OFFSET {skip}",
                coll.schema,
                coll.table,
                w.render()
            );
            let mut conn = app.conn(tm).await?;
            let t = Instant::now();
            let rows: Vec<Row> = conn.exec(log_sql(sql), Params::Positional(w.params)).await?;
            tm.sql += ns(t);
            drop(conn);
            let mut rows: VecDeque<Row> = rows.into();
            let proj = get_doc(body, "projection")?;
            let first_n = if single { rows.len() } else { bsize };
            let arr = batch(&mut rows, first_n, proj, tm)?;
            let ns_name = format!("{db}.{cname}");
            let mut id = 0i64;
            if !rows.is_empty() && !single {
                id = app.next_cursor.fetch_add(1, Relaxed);
                app.cursors.lock().unwrap().insert(id, Cursor { ns: ns_name.clone(), rows, proj: proj.map(|p| p.to_raw_document_buf()) });
            }
            let mut cur = RawDocumentBuf::new();
            cur.append("firstBatch", arr);
            cur.append("id", id);
            cur.append("ns", ns_name);
            let mut out = RawDocumentBuf::new();
            out.append("cursor", cur);
            out.append("ok", 1.0);
            Ok((1, out))
        }
        "getMore" => {
            let RawBsonRef::Int64(id) = first else { bail!("getMore: cursor id") };
            let bsize = get_i64(body, "batchSize")?.unwrap_or(1_000_000).max(1) as usize;
            let c = app.cursors.lock().unwrap().remove(&id);
            let Some(mut c) = c else {
                return Ok((4, rawdoc! {"ok": 0.0, "code": 43i32, "codeName": "CursorNotFound", "errmsg": "cursor not found"}));
            };
            let arr = batch(&mut c.rows, bsize, c.proj.as_deref(), tm)?;
            let mut cur = RawDocumentBuf::new();
            cur.append("nextBatch", arr);
            let ns_name = c.ns.clone();
            let next = if c.rows.is_empty() {
                0
            } else {
                app.cursors.lock().unwrap().insert(id, c);
                id
            };
            cur.append("id", next);
            cur.append("ns", ns_name);
            let mut out = RawDocumentBuf::new();
            out.append("cursor", cur);
            out.append("ok", 1.0);
            Ok((4, out))
        }
        "killCursors" => {
            if let Some(RawBsonRef::Array(a)) = body.get("cursors")? {
                let mut m = app.cursors.lock().unwrap();
                for x in a {
                    if let RawBsonRef::Int64(id) = x? {
                        m.remove(&id);
                    }
                }
            }
            Ok((5, rawdoc! {"ok": 1.0}))
        }
        "update" => {
            let RawBsonRef::String(cname) = first else { bail!("update: collection name") };
            let coll = app.coll(db, cname, tm).await?;
            let fields = coll.fields.read().unwrap().clone();
            let (mut n, mut nmod) = (0i64, 0i64);
            for u in docs(body, seqs, "updates")? {
                let t = Instant::now();
                let q = get_doc(u, "q")?;
                let spec = get_doc(u, "u")?.context("update: u must be a document (pipelines unsupported)")?;
                let multi = matches!(u.get("multi")?, Some(RawBsonRef::Boolean(true)));
                if matches!(u.get("upsert")?, Some(RawBsonRef::Boolean(true))) {
                    bail!("spike: upsert unsupported");
                }
                let w = translate::filter(q, &fields)?;
                let is_ops = spec.iter().next().transpose()?.is_some_and(|(k, _)| k.starts_with('$'));
                let (sql, params) = if is_ops {
                    let up = translate::update(spec)?;
                    let mut params = vec![Value::UInt(app.stamp())];
                    params.extend(up.params);
                    params.extend(w.params);
                    let mut wh = w.sql;
                    wh.extend(up.guards);
                    let wh = Where { sql: wh };
                    (
                        format!(
                            "UPDATE `{}`.`{}` SET _v = GREATEST(?, _v + 1), {} WHERE {}{}",
                            coll.schema,
                            coll.table,
                            up.set_sql,
                            wh.render(),
                            if multi { "" } else { " LIMIT 1" }
                        ),
                        params,
                    )
                } else {
                    // replacement: re-render the whole document, keeping the stored _id
                    let s = json::to_stored(spec)?;
                    let mut params = vec![Value::UInt(app.stamp()), Value::Bytes(s.doc.into_bytes()), s.types.map(|t| Value::Bytes(t.into_bytes())).unwrap_or(Value::NULL)];
                    params.extend(w.params);
                    (
                        format!(
                            "UPDATE `{}`.`{}` SET _v = GREATEST(?, _v + 1), doc = ?, _types = ? WHERE {} LIMIT 1",
                            coll.schema,
                            coll.table,
                            Where { sql: w.sql }.render()
                        ),
                        params,
                    )
                };
                tm.conv += ns(t);
                let mut conn = app.conn(tm).await?;
                let t = Instant::now();
                conn.exec_drop(log_sql(sql), Params::Positional(params)).await?;
                let (m, c) = parse_info(&conn.info());
                tm.sql += ns(t);
                n += m;
                nmod += c;
            }
            Ok((2, rawdoc! {"n": n as i32, "nModified": nmod as i32, "ok": 1.0}))
        }
        "delete" => {
            let RawBsonRef::String(cname) = first else { bail!("delete: collection name") };
            let coll = app.coll(db, cname, tm).await?;
            let fields = coll.fields.read().unwrap().clone();
            let mut n = 0u64;
            for d in docs(body, seqs, "deletes")? {
                let w = translate::filter(get_doc(d, "q")?, &fields)?;
                let one = get_i64(d, "limit")?.unwrap_or(0) == 1;
                let sql = format!("DELETE FROM `{}`.`{}` WHERE {}{}", coll.schema, coll.table, w.render(), if one { " LIMIT 1" } else { "" });
                let mut conn = app.conn(tm).await?;
                let t = Instant::now();
                conn.exec_drop(log_sql(sql), Params::Positional(w.params)).await?;
                n += conn.affected_rows();
                tm.sql += ns(t);
            }
            Ok((3, rawdoc! {"n": n as i32, "ok": 1.0}))
        }
        "drop" => {
            let RawBsonRef::String(cname) = first else { bail!("drop: collection name") };
            let mut conn = app.conn(tm).await?;
            conn.query_drop(format!("DROP TABLE IF EXISTS `{db}`.`{cname}`")).await?;
            app.colls.lock().unwrap().remove(&format!("{db}.{cname}"));
            Ok((5, rawdoc! {"ok": 1.0}))
        }
        "create" => {
            let RawBsonRef::String(cname) = first else { bail!("create: collection name") };
            app.coll(db, cname, tm).await?;
            Ok((5, rawdoc! {"ok": 1.0}))
        }
        "createIndexes" => {
            let RawBsonRef::String(cname) = first else { bail!("createIndexes: collection name") };
            let coll = app.coll(db, cname, tm).await?;
            let mut conn = app.conn(tm).await?;
            for spec in docs(body, seqs, "indexes")? {
                let key = get_doc(spec, "key")?.context("index key")?;
                let iname = get_str(spec, "name").unwrap_or("ix");
                let mut cols = vec![];
                for e in key {
                    let (path, dir) = e?;
                    if path == "_id" {
                        cols.push("_id".to_string());
                        continue;
                    }
                    translate::check_path(path)?;
                    let class = match spec.get("sprigClass")? {
                        Some(RawBsonRef::String(c)) => Class::parse(c).context("sprigClass")?,
                        _ => sample_class(&mut conn, &coll, path).await?,
                    };
                    let existing = coll.fields.read().unwrap().iter().find(|f| f.path == path && f.class == class).cloned();
                    let fc = match existing {
                        Some(fc) => fc,
                        None => {
                            let slug: String = path.replace('.', "_").chars().take(40).collect();
                            let fc = FieldCol { path: path.to_string(), class, col: format!("_sprig_p_{slug}") };
                            conn.query_drop(format!("ALTER TABLE `{}`.`{}` ADD COLUMN IF NOT EXISTS {}", coll.schema, coll.table, translate::column_ddl(&fc))).await?;
                            coll.fields.write().unwrap().push(fc.clone());
                            fc
                        }
                    };
                    let desc = matches!(dir, RawBsonRef::Int32(-1) | RawBsonRef::Int64(-1)) || matches!(dir, RawBsonRef::Double(f) if f < 0.0);
                    cols.push(format!("`{}`{}", fc.col, if desc { " DESC" } else { "" }));
                }
                let ixname: String = format!("sprig_dix_{}", iname.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_').take(40).collect::<String>());
                conn.query_drop(format!("ALTER TABLE `{}`.`{}` ADD INDEX IF NOT EXISTS `{ixname}` ({})", coll.schema, coll.table, cols.join(", "))).await?;
            }
            Ok((5, rawdoc! {"ok": 1.0}))
        }
        other => {
            eprintln!("spike: unhandled command {other}, answering ok");
            Ok((5, rawdoc! {"ok": 1.0}))
        }
    }
}

struct Where {
    sql: Vec<String>,
}
impl Where {
    fn render(&self) -> String {
        if self.sql.is_empty() {
            "TRUE".into()
        } else {
            self.sql.join(" AND ")
        }
    }
}

/// SPIKE_LOG_SQL=1 prints each distinct statement once (to quote the generated SQL).
fn log_sql(sql: String) -> String {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    static SEEN: std::sync::OnceLock<Mutex<std::collections::HashSet<String>>> = std::sync::OnceLock::new();
    if *ON.get_or_init(|| std::env::var("SPIKE_LOG_SQL").is_ok()) && SEEN.get_or_init(Default::default).lock().unwrap().insert(sql.clone()) {
        eprintln!("spike: sql: {sql}");
    }
    sql
}

async fn sample_class(conn: &mut mysql_async::Conn, coll: &Coll, path: &str) -> Result<Class> {
    let (j, t) = (translate::jpath(path), translate::tpath(path));
    let r: Option<(Option<String>, Option<String>)> = conn
        .query_first(format!(
            "SELECT JSON_TYPE(JSON_EXTRACT(doc,'{j}')), JSON_VALUE(_types,'{t}') FROM `{}`.`{}` WHERE JSON_EXTRACT(doc,'{j}') IS NOT NULL LIMIT 1",
            coll.schema, coll.table
        ))
        .await?;
    Ok(match r {
        Some((_, Some(e))) if e == "objectId" => Class::ObjectId,
        Some((_, Some(e))) if e == "date" => Class::Date,
        Some((_, Some(e))) if e == "long" => Class::Integer,
        Some((Some(j), _)) if j == "INTEGER" => Class::Integer,
        Some((Some(j), _)) if j == "DOUBLE" => Class::Number,
        Some((Some(j), _)) if j == "BOOLEAN" => Class::Bool,
        _ => Class::String,
    })
}

async fn serve(app: Arc<App>, stream: TcpStream) -> Result<()> {
    stream.set_nodelay(true)?;
    let conn_id = app.next_conn.fetch_add(1, Relaxed);
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::with_capacity(64 * 1024, rd);
    let mut buf = Vec::with_capacity(64 * 1024);
    while let Some(msg) = wire::read_msg(&mut rd, &mut buf).await? {
        let t0 = Instant::now();
        let mut tm = Timing::default();
        let (reply, kind) = match msg {
            wire::Msg::Query { request_id, query } => {
                let (k, d) = run_cmd(&app, conn_id, &query, &[], &mut tm).await.unwrap_or_else(err_doc);
                (wire::op_reply(request_id, d.as_bytes()), k)
            }
            wire::Msg::OpMsg { request_id, body, seqs, more_to_come } => {
                let (k, d) = run_cmd(&app, conn_id, &body, &seqs, &mut tm).await.unwrap_or_else(err_doc);
                if more_to_come {
                    continue;
                }
                (wire::op_msg_reply(request_id, d.as_bytes()), k)
            }
        };
        wr.write_all(&reply).await?;
        let s = &app.stats[kind];
        s.n.fetch_add(1, Relaxed);
        s.total.fetch_add(ns(t0), Relaxed);
        s.sql.fetch_add(tm.sql, Relaxed);
        s.pool.fetch_add(tm.pool, Relaxed);
        s.conv.fetch_add(tm.conv, Relaxed);
    }
    Ok(())
}

fn err_doc(e: anyhow::Error) -> (usize, RawDocumentBuf) {
    eprintln!("spike: error: {e:#}");
    (5, rawdoc! {"ok": 0.0, "errmsg": format!("{e:#}"), "code": 8i32, "codeName": "UnknownError"})
}

async fn stats_server(app: Arc<App>, addr: String) -> Result<()> {
    let l = TcpListener::bind(&addr).await?;
    loop {
        let (mut s, _) = l.accept().await?;
        let mut out = String::from("op n avg_total_us avg_sql_us avg_pool_us avg_conv_us avg_rest_us\n");
        for (i, st) in app.stats.iter().enumerate() {
            let n = st.n.swap(0, Relaxed);
            let (t, q, p, c) = (st.total.swap(0, Relaxed), st.sql.swap(0, Relaxed), st.pool.swap(0, Relaxed), st.conv.swap(0, Relaxed));
            if n == 0 {
                continue;
            }
            let us = |x: u64| x as f64 / n as f64 / 1000.0;
            out.push_str(&format!("{} {n} {:.1} {:.1} {:.1} {:.1} {:.1}\n", OPS[i], us(t), us(q), us(p), us(c), us(t.saturating_sub(q + p + c))));
        }
        let _ = s.write_all(out.as_bytes()).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listen = std::env::var("SPIKE_LISTEN").unwrap_or_else(|_| "0.0.0.0:27017".into());
    let url = std::env::var("SPIKE_MARIADB").unwrap_or_else(|_| "mysql://root:spike@127.0.0.1:13399/".into());
    let max: usize = std::env::var("SPIKE_POOL_MAX").ok().and_then(|s| s.parse().ok()).unwrap_or(128);
    let opts = OptsBuilder::from_opts(Opts::from_url(&url)?)
        .pool_opts(
            // mysql_async defaults would close every idle connection above `min` on return
            // (inactive_connection_ttl = 0) and send COM_RESET_CONNECTION on every return,
            // which also drops the server-side prepared statements.
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(8, max).unwrap())
                .with_inactive_connection_ttl(std::time::Duration::from_secs(600))
                .with_reset_connection(false),
        )
        .stmt_cache_size(512)
        .prefer_socket(false);
    let app = Arc::new(App {
        pool: Pool::new(opts),
        colls: Mutex::new(HashMap::new()),
        cursors: Mutex::new(HashMap::new()),
        next_cursor: AtomicI64::new(1 << 40),
        next_conn: AtomicI64::new(1),
        hlc: AtomicU64::new(0),
        stats: Default::default(),
    });
    let stats_addr = std::env::var("SPIKE_STATS").unwrap_or_else(|_| "0.0.0.0:27100".into());
    tokio::spawn(stats_server(app.clone(), stats_addr));
    let l = TcpListener::bind(&listen).await?;
    eprintln!("spike: listening on {listen}, MariaDB {url}, pool max {max}");
    loop {
        let (s, _) = l.accept().await?;
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(e) = serve(app, s).await {
                eprintln!("spike: connection closed: {e:#}");
            }
        });
    }
}
