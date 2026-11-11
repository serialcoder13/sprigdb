//! `language` on `Sql.Query` and `Sql.Execute` (chapter 37, "Languages";
//! tui decision T2): Studio and `sprig` reach graphs and documents over the
//! one statement API.
//!
//! - **`sql`** (or empty): the executor, as before ([`SqlEngine::run_capped`]).
//! - **`cypher`, `gql`**: the graph executor (chapter 43) through a Bolt
//!   session of the caller's principal ([`crate::bolt::BoltGraphService::api_session`]):
//!   the same parser (GQL's forms are rewritten into Cypher, so both names
//!   reach it), graphs, privileges, budgets and stale-map retries as a Bolt
//!   `RUN` in auto-commit. `database` is Bolt's `db`. One result: a header
//!   with the statement's fields, rows, and an end whose `info` lists the
//!   update counters (`nodes-created: 2, ...`) and whose `affected_rows` is
//!   their sum. Cells are text for strings, numbers and booleans, and JSON
//!   for everything else, with nodes, relationships and paths as
//!   `{"$node"}`, `{"$rel"}` and `{"$path"}` (what `sprig --protocol bolt`
//!   shows); a column's type comes from its first value (`BIGINT`,
//!   `DOUBLE`, `BOOLEAN`, `VARCHAR`, else `JSON`).
//! - **`mongo`**: `sql` holds one MongoDB command document as JSON
//!   (Extended JSON v2: `{"find": "users", "filter": {"age": {"$gt": 30}}}`),
//!   run in `database` through the MongoDB front end's command path
//!   ([`crate::mongo::conn::Conn::guarded_dispatch`]) as the caller: the
//!   document engine, its privileges, its stall guard. It is data, never
//!   code: the engine refuses `$where`, and JavaScript has no JSON form.
//!   A cursor's documents are drained with `getMore`, one row each in a
//!   single `document` column (relaxed Extended JSON, or canonical with
//!   `output = canonical_ejson`, which keeps int32, int64 and double
//!   apart; [`with_output`]); any other reply is
//!   one row (without `ok`, `$clusterTime` and `operationTime`).
//!   Authentication and fail-point commands are refused (39794).
//!
//! Caps apply as for SQL: `max_rows` and `max_bytes` end the result with
//! `truncated` (the graph stream is dropped, the cursor killed),
//! `timeout_ms` ends the call with 3024. A client that goes away stops the
//! statement (tui T3): the graph session's connections close, the MongoDB
//! command is interrupted. Transaction handles hold SQL sessions, so these
//! languages take no `transaction` (39794), and no `multi_statements`.
//!
//! Errors keep SprigDB's and MySQL's numbers where the other language's
//! error has one (a syntax error is 39900, a missing graph 39902, access
//! 1142, a duplicate key 1062); the rest are 39795 with the language's own
//! code in the message.

use std::fmt::Write as _;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};

use bson::{Bson, Document, RawDocumentBuf, doc};
use prost_types::value::Kind;
use prost_types::{ListValue, Value as PValue};
use serde_json::{Map, Value as Json, json};
use sprig_api::proto::v1::{
    Column, QueryResponse, ResultEnd, ResultHeader, RowBatch, query_response,
};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::server::{BoltFailure, BoltSession};
use sprig_proto_mongo::codec::OpMsg;
use tokio::sync::mpsc;

use super::sink::{BATCH_BYTES, Item};
use super::sql::{Caller, Caps, SqlCall, SqlEngine};
use crate::frontend::Protocol;
use crate::session::SessionState;

/// A statement language of `Sql.Query`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    /// SQL (the default).
    Sql,
    /// openCypher.
    Cypher,
    /// GQL (ISO/IEC 39075), rewritten into Cypher.
    Gql,
    /// One MongoDB command document as JSON.
    Mongo,
}

impl Language {
    /// `language` as a request names it; empty is SQL.
    ///
    /// # Errors
    /// 39794 for any other name.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "sql" => Ok(Self::Sql),
            "cypher" => Ok(Self::Cypher),
            "gql" => Ok(Self::Gql),
            "mongo" => Ok(Self::Mongo),
            other => Err(invalid(format!(
                "unknown language {other:?}: use sql, cypher, gql or mongo"
            ))),
        }
    }

    /// The name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::Cypher => "cypher",
            Self::Gql => "gql",
            Self::Mongo => "mongo",
        }
    }
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::QueryLanguageInvalid, m)
}

/// `language` and `output` of a request as one text for [`SqlEngine::run_in`]:
/// `mongo;output=canonical_ejson` (chapter 37's `Documents` option, which
/// Studio's JSON tree asks for to show exact BSON types). Empty `output`
/// leaves `language` as it is.
#[must_use]
pub fn with_output(language: &str, output: &str) -> String {
    if output.trim().is_empty() {
        language.to_owned()
    } else {
        format!("{};output={}", language.trim(), output.trim())
    }
}

/// How documents are written: relaxed (the default) or canonical Extended
/// JSON v2.
///
/// # Errors
/// 39794 for an unknown output, or an output for another language than
/// `mongo`.
fn split_output(spec: &str) -> Result<(&str, bool)> {
    let Some((lang, out)) = spec.split_once(";output=") else {
        return Ok((spec, false));
    };
    let canonical = match out.trim().to_ascii_lowercase().as_str() {
        "" | "relaxed_ejson" => false,
        "canonical_ejson" => true,
        other => {
            return Err(invalid(format!(
                "unknown output {other:?}: use relaxed_ejson or canonical_ejson"
            )));
        }
    };
    if canonical && Language::parse(lang)? != Language::Mongo {
        return Err(invalid("output = canonical_ejson is for language mongo"));
    }
    Ok((lang, canonical))
}

/// The front ends other languages run through, set once they start.
#[derive(Default)]
pub struct Languages {
    graphs: OnceLock<Arc<crate::bolt::BoltGraphService>>,
    mongo: OnceLock<Arc<crate::mongo::Shared>>,
}

impl std::fmt::Debug for Languages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Languages")
            .field("graphs", &self.graphs.get().is_some())
            .field("mongo", &self.mongo.get().is_some())
            .finish()
    }
}

impl Languages {
    /// Cypher and GQL go through `svc`.
    pub fn set_graphs(&self, svc: Arc<crate::bolt::BoltGraphService>) {
        let _ = self.graphs.set(svc);
    }

    /// MongoDB commands go through `shared`.
    pub fn set_mongo(&self, shared: Arc<crate::mongo::Shared>) {
        let _ = self.mongo.set(shared);
    }
}

// ----------------------------------------------------------------------
// One result on the channel, with the caps.
// ----------------------------------------------------------------------

struct Out<'a> {
    tx: &'a mpsc::Sender<Item>,
    batch_rows: usize,
    caps: Caps,
    batch: Vec<ListValue>,
    batch_bytes: usize,
    rows: u64,
    bytes: u64,
    truncated: bool,
}

/// The client went away (the channel's reader dropped).
struct Gone;

impl<'a> Out<'a> {
    fn new(tx: &'a mpsc::Sender<Item>, batch_rows: usize, caps: Caps) -> Self {
        Self {
            tx,
            batch_rows: batch_rows.max(1),
            caps,
            batch: Vec::new(),
            batch_bytes: 0,
            rows: 0,
            bytes: 0,
            truncated: false,
        }
    }

    async fn send(&self, part: query_response::Part) -> std::result::Result<(), Gone> {
        self.tx
            .send(Item::Part(QueryResponse { part: Some(part) }))
            .await
            .map_err(|_| Gone)
    }

    async fn header(&self, columns: Vec<Column>) -> std::result::Result<(), Gone> {
        self.send(query_response::Part::Header(ResultHeader {
            index: 0,
            columns,
        }))
        .await
    }

    /// Adds a row; `false` once a cap is reached (the row is left out).
    async fn row(&mut self, cells: Vec<Option<String>>) -> std::result::Result<bool, Gone> {
        let size: usize = cells.iter().flatten().map(String::len).sum();
        let size64 = u64::try_from(size).unwrap_or(u64::MAX);
        if (self.caps.max_rows > 0 && self.rows >= self.caps.max_rows)
            || (self.caps.max_bytes > 0 && self.bytes.saturating_add(size64) > self.caps.max_bytes)
        {
            self.truncated = true;
            return Ok(false);
        }
        self.rows += 1;
        self.bytes = self.bytes.saturating_add(size64);
        self.batch.push(ListValue {
            values: cells
                .into_iter()
                .map(|c| PValue {
                    kind: Some(c.map_or(Kind::NullValue(0), Kind::StringValue)),
                })
                .collect(),
        });
        self.batch_bytes += size;
        if self.batch.len() >= self.batch_rows || self.batch_bytes >= BATCH_BYTES {
            self.flush().await?;
        }
        Ok(true)
    }

    async fn flush(&mut self) -> std::result::Result<(), Gone> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.batch);
        self.batch_bytes = 0;
        self.send(query_response::Part::Rows(RowBatch { rows }))
            .await
    }

    async fn end(mut self, affected_rows: u64, info: String) -> std::result::Result<(), Gone> {
        self.flush().await?;
        if self.truncated {
            metrics::counter!("sprig_api_results_truncated_total").increment(1);
        }
        self.send(query_response::Part::End(ResultEnd {
            index: 0,
            affected_rows,
            info,
            truncated: self.truncated,
            ..ResultEnd::default()
        }))
        .await
    }
}

fn column(name: &str, type_name: &str) -> Column {
    let mysql_type = match type_name {
        "BIGINT" => 8,
        "DOUBLE" => 5,
        "BOOLEAN" => 1,
        "VARCHAR" => 253,
        _ => 245,
    };
    Column {
        name: name.to_owned(),
        org_name: name.to_owned(),
        r#type: type_name.to_owned(),
        mysql_type,
        charset: 255,
        ..Column::default()
    }
}

// ----------------------------------------------------------------------
// The entry point.
// ----------------------------------------------------------------------

impl SqlEngine {
    /// Runs `call` in `language` (empty: SQL) for `caller`, writing to `tx`
    /// as [`SqlEngine::run_capped`] does.
    pub async fn run_in(
        &self,
        caller: &Caller,
        language: &str,
        call: SqlCall,
        txn: Option<&str>,
        caps: Caps,
        tx: mpsc::Sender<Item>,
    ) {
        let (lang, canonical) =
            match split_output(language).and_then(|(l, c)| Ok((Language::parse(l)?, c))) {
                Ok((Language::Sql, _)) => {
                    return self.run_capped(caller, call, txn, caps, tx).await;
                }
                Ok(l) => l,
                Err(e) => {
                    let _ = tx.send(Item::Error(e)).await;
                    return;
                }
            };
        let r = self
            .run_other(caller, lang, canonical, call, txn, caps, &tx)
            .await;
        if let Err(e) = r
            && !tx.is_closed()
        {
            let _ = tx.send(Item::Error(e)).await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_other(
        &self,
        caller: &Caller,
        lang: Language,
        canonical: bool,
        call: SqlCall,
        txn: Option<&str>,
        caps: Caps,
        tx: &mpsc::Sender<Item>,
    ) -> Result<()> {
        if txn.is_some_and(|t| !t.is_empty()) {
            return Err(invalid(format!(
                "{} statements take no transaction handle: handles hold SQL sessions; use Bolt's or MongoDB's own transactions",
                lang.name()
            )));
        }
        if call.multi {
            return Err(invalid(format!(
                "multi_statements is for SQL; send one {} statement per call",
                lang.name()
            )));
        }
        if caller.principal.tenant.is_none() {
            return Err(invalid(format!(
                "{} needs a tenant account (catalog login mode)",
                lang.name()
            )));
        }
        let batch = self.settings().batch_rows;
        let work = async {
            match lang {
                Language::Mongo => {
                    self.run_mongo(caller, &call, caps, batch, canonical, tx)
                        .await
                }
                _ => self.run_graph(caller, &call, caps, batch, tx).await,
            }
        };
        match caps.timeout {
            Some(t) => tokio::time::timeout(t, work).await.unwrap_or_else(|_| {
                Err(SprigError::standard(
                    3024,
                    *b"HY000",
                    "Query execution was interrupted, maximum statement execution time exceeded",
                ))
            }),
            None => work.await,
        }
    }

    // ------------------------------------------------------------------
    // Cypher and GQL.
    // ------------------------------------------------------------------

    #[allow(clippy::too_many_lines)] // one result's whole life, in order
    async fn run_graph(
        &self,
        caller: &Caller,
        call: &SqlCall,
        caps: Caps,
        batch: usize,
        tx: &mpsc::Sender<Item>,
    ) -> Result<()> {
        let unserved = || {
            SprigError::new(
                SprigCode::GraphUnsupported,
                "this gateway serves no graphs (it has no tenant catalogs)",
            )
        };
        let svc = self.languages.graphs.get().ok_or_else(unserved)?;
        let mut gs = svc
            .api_session(caller.principal.clone())
            .ok_or_else(unserved)?;
        let mut reg = self.sessions.open_parts(
            Protocol::Http,
            caller.listener,
            caller.generation,
            caller.peer,
            caller.tls.clone(),
        )?;
        reg.set_principal(caller.principal.clone());
        reg.client_dc = caller.client_dc.clone();
        reg.set_state(SessionState::Ready);
        gs.attach(reg);
        let mut extra: Vec<(&str, PackValue)> = Vec::new();
        if !(call.database.is_empty() || call.database == "-") {
            extra.push(("db", PackValue::str(call.database.clone())));
        }
        if let Some(t) = caps.timeout {
            extra.push((
                "tx_timeout",
                PackValue::Int(i64::try_from(t.as_millis()).unwrap_or(i64::MAX)),
            ));
        }
        let extra = PackValue::map(extra);
        let params = PackValue::Map(Vec::new());
        let ran = tokio::select! {
            r = gs.run(&call.sql, &params, &extra) => Some(r),
            () = tx.closed() => None,
        };
        let Some(ran) = ran else {
            // The client left while the statement ran: what it held is
            // dropped (connections close and roll back).
            gs.reset().await;
            return Ok(());
        };
        let mut qr = ran.map_err(|f| from_failure(&f))?;
        let mut out = Out::new(tx, batch, caps);
        let mut stats = std::mem::take(&mut qr.stats);
        let r: std::result::Result<(), Gone> = async {
            if !qr.fields.is_empty() {
                let cols = qr
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        let t = qr
                            .records
                            .iter()
                            .find_map(|r| r.get(i).and_then(type_of))
                            .unwrap_or("JSON");
                        column(f, t)
                    })
                    .collect();
                out.header(cols).await?;
            }
            let mut open = true;
            while let Some(rec) = qr.records.pop_front() {
                if !out.row(rec.iter().map(cell).collect()).await? {
                    open = false;
                    break;
                }
            }
            if let Some(mut st) = qr.stream.take() {
                while open {
                    let next = tokio::select! {
                        n = st.next() => n,
                        () = tx.closed() => return Err(Gone),
                    };
                    match next {
                        Ok(Some(rec)) => {
                            if !out.row(rec.iter().map(cell).collect()).await? {
                                open = false;
                            }
                        }
                        Ok(None) => {
                            stats = st.end().stats;
                            break;
                        }
                        Err(f) => {
                            // Sent as the call's error below.
                            let _ = tx.send(Item::Error(from_failure(&f))).await;
                            return Err(Gone);
                        }
                    }
                }
            }
            if out.truncated {
                // The rest of the stream is dropped with its connections.
                gs.reset().await;
            }
            let affected = stats.values().map(|v| u64::try_from(*v).unwrap_or(0)).sum();
            let info = stats
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect::<Vec<_>>()
                .join(", ");
            out.end(affected, info).await
        }
        .await;
        if r.is_err() {
            gs.reset().await;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // MongoDB commands.
    // ------------------------------------------------------------------

    async fn run_mongo(
        &self,
        caller: &Caller,
        call: &SqlCall,
        caps: Caps,
        batch: usize,
        canonical: bool,
        tx: &mpsc::Sender<Item>,
    ) -> Result<()> {
        let shared = self.languages.mongo.get().ok_or_else(|| {
            SprigError::new(
                SprigCode::QueryLanguageInvalid,
                "this gateway runs no document engine",
            )
        })?;
        if call.database.is_empty() || call.database == "-" {
            return Err(invalid("a MongoDB command needs a database"));
        }
        let mut cmd = command_doc(&call.sql)?;
        let name = cmd.keys().next().cloned().unwrap_or_default();
        if REFUSED.contains(&name.as_str()) {
            return Err(invalid(format!(
                "{name} isn't available through Sql.Query: the call is already authenticated"
            )));
        }
        cmd.insert("$db", call.database.clone());
        let mut conn = crate::mongo::conn::Conn {
            shared: Arc::clone(shared),
            id: shared.conn_ids.fetch_add(1, Ordering::SeqCst),
            peer: caller.peer,
            listener: caller.listener,
            generation: caller.generation,
            tls: caller.tls.clone(),
            principal: Some(caller.principal.clone()),
            scram: None,
            client_meta: None,
            compressors: Vec::new(),
            sql: None,
            dc: None,
            restore: false,
            hello_done: true,
            close_now: false,
            raw: None,
        };
        // The executor session now, so a client that leaves can stop the
        // first command too.
        conn.sql()
            .map_err(|e| SprigError::new(SprigCode::QueryLanguageInvalid, e.to_string()))?;
        let r = mongo_command(&mut conn, cmd, &call.database, caps, batch, canonical, tx).await;
        if let Some(s) = conn.sql.take() {
            s.close().await;
        }
        r
    }
}

/// Commands an API call can't run: the call is authenticated already, and
/// fail points are for the drivers' test suites.
const REFUSED: &[&str] = &[
    "saslStart",
    "saslContinue",
    "authenticate",
    "logout",
    "configureFailPoint",
];

/// The command document `text` holds (Extended JSON v2).
fn command_doc(text: &str) -> Result<Document> {
    let j: Json = serde_json::from_str(text).map_err(|e| {
        invalid(format!(
            "a MongoDB command is one JSON document (Extended JSON v2): {e}"
        ))
    })?;
    match Bson::try_from(j) {
        Ok(Bson::Document(d)) if !d.is_empty() => Ok(d),
        Ok(_) => Err(invalid(
            "a MongoDB command is a non-empty JSON object naming the command first",
        )),
        Err(e) => Err(invalid(format!("not Extended JSON: {e}"))),
    }
}

/// Runs one command; `None` when the client left first (the command is
/// interrupted).
async fn dispatch(
    conn: &mut crate::mongo::conn::Conn,
    cmd: &Document,
    tx: &mpsc::Sender<Item>,
) -> Result<Option<Document>> {
    let body = RawDocumentBuf::from_document(cmd)
        .map_err(|e| invalid(format!("the command can't be encoded: {e}")))?;
    let size = body.as_bytes().len();
    let msg = OpMsg {
        flags: 0,
        body,
        sequences: Vec::new(),
    };
    let handle = conn.sql.as_ref().map(crate::mongo::sql::SqlSession::handle);
    let fut = conn.guarded_dispatch(msg, size);
    tokio::pin!(fut);
    tokio::select! {
        (_, d) = &mut fut => Ok(Some(d)),
        () = tx.closed() => {
            if let Some(h) = handle {
                h.flags().kill_query();
            }
            let _ = fut.await;
            Ok(None)
        }
    }
}

async fn mongo_command(
    conn: &mut crate::mongo::conn::Conn,
    cmd: Document,
    db: &str,
    caps: Caps,
    batch: usize,
    canonical: bool,
    tx: &mpsc::Sender<Item>,
) -> Result<()> {
    let Some(reply) = dispatch(conn, &cmd, tx).await? else {
        return Ok(());
    };
    check_reply(&reply)?;
    let mut out = Out::new(tx, batch, caps);
    let Ok(cursor) = reply.get_document("cursor") else {
        // One reply document.
        let affected = reply
            .get("n")
            .and_then(sprig_doc::filter::as_i64)
            .and_then(|n| u64::try_from(n).ok())
            .unwrap_or(0);
        let mut d = reply;
        for k in ["ok", "$clusterTime", "operationTime"] {
            d.remove(k);
        }
        let r: std::result::Result<(), Gone> = async {
            out.header(vec![column("document", "JSON")]).await?;
            out.row(vec![Some(ext_json(Bson::Document(d), canonical))])
                .await?;
            out.end(affected, String::new()).await
        }
        .await;
        // A gone client (`Err`) has nothing more to hear.
        drop(r);
        return Ok(());
    };
    let ns = cursor.get_str("ns").unwrap_or_default().to_owned();
    let coll = ns
        .split_once('.')
        .map_or(ns.as_str(), |(_, c)| c)
        .to_owned();
    let mut id = cursor.get_i64("id").unwrap_or(0);
    let mut docs = cursor.get_array("firstBatch").cloned().unwrap_or_default();
    if out.header(vec![column("document", "JSON")]).await.is_err() {
        kill_cursor(conn, db, &coll, id, tx).await;
        return Ok(());
    }
    loop {
        for d in docs {
            match out.row(vec![Some(ext_json(d, canonical))]).await {
                Ok(true) => {}
                Ok(false) => {
                    kill_cursor(conn, db, &coll, id, tx).await;
                    let _ = out.end(0, String::new()).await;
                    return Ok(());
                }
                Err(Gone) => {
                    kill_cursor(conn, db, &coll, id, tx).await;
                    return Ok(());
                }
            }
        }
        if id == 0 {
            break;
        }
        let more = doc! {"getMore": id, "collection": coll.clone(), "$db": db};
        let Some(r) = dispatch(conn, &more, tx).await? else {
            kill_cursor(conn, db, &coll, id, tx).await;
            return Ok(());
        };
        check_reply(&r)?;
        let c = r.get_document("cursor").cloned().unwrap_or_default();
        id = c.get_i64("id").unwrap_or(0);
        docs = c.get_array("nextBatch").cloned().unwrap_or_default();
    }
    let _ = out.end(0, String::new()).await;
    Ok(())
}

async fn kill_cursor(
    conn: &mut crate::mongo::conn::Conn,
    db: &str,
    coll: &str,
    id: i64,
    tx: &mpsc::Sender<Item>,
) {
    if id == 0 {
        return;
    }
    let _ = tx; // the kill runs whether or not the client is still there
    let cmd = doc! {"killCursors": coll, "cursors": [id], "$db": db};
    if let Ok(body) = RawDocumentBuf::from_document(&cmd) {
        let size = body.as_bytes().len();
        let _ = conn
            .guarded_dispatch(
                OpMsg {
                    flags: 0,
                    body,
                    sequences: Vec::new(),
                },
                size,
            )
            .await;
    }
}

fn ext_json(b: Bson, canonical: bool) -> String {
    if canonical {
        b.into_canonical_extjson().to_string()
    } else {
        b.into_relaxed_extjson().to_string()
    }
}

/// A reply's error, or its first write error, as a [`SprigError`].
fn check_reply(reply: &Document) -> Result<()> {
    let ok = match reply.get("ok") {
        Some(Bson::Double(f)) => (*f - 1.0).abs() < f64::EPSILON,
        Some(Bson::Int32(i)) => *i == 1,
        Some(Bson::Int64(i)) => *i == 1,
        Some(Bson::Boolean(b)) => *b,
        _ => true,
    };
    if !ok {
        return Err(mongo_error(reply));
    }
    for key in ["writeErrors", "writeConcernError"] {
        match reply.get(key) {
            Some(Bson::Array(a)) => {
                if let Some(Bson::Document(e)) = a.first() {
                    return Err(mongo_error(e));
                }
            }
            Some(Bson::Document(e)) => return Err(mongo_error(e)),
            _ => {}
        }
    }
    Ok(())
}

/// A MongoDB error document as SprigDB's error: SprigDB codes keep their
/// number, a few MongoDB codes become MySQL's, the rest 39795.
fn mongo_error(e: &Document) -> SprigError {
    let code = e
        .get("code")
        .and_then(sprig_doc::filter::as_i64)
        .unwrap_or(0);
    let name = e.get_str("codeName").unwrap_or("");
    let msg = e
        .get_str("errmsg")
        .unwrap_or("the command failed")
        .to_owned();
    if let Ok(n) = u16::try_from(code)
        && let Some(c) = SprigCode::ALL.iter().find(|c| c.number() == n)
    {
        return SprigError::new(*c, msg);
    }
    match code {
        11000 => SprigError::standard(1062, *b"23000", msg),
        13 => SprigError::standard(1142, *b"42000", msg),
        18 => SprigError::standard(1045, *b"28000", msg),
        _ => SprigError::new(
            SprigCode::LanguageStatementFailed,
            format!("{msg} (MongoDB error {code} {name})"),
        ),
    }
}

// ----------------------------------------------------------------------
// Graph values and errors.
// ----------------------------------------------------------------------

/// A Bolt failure as SprigDB's error: back to the code it came from where
/// one exists, else 39795 with the Neo4j status code in the message.
fn from_failure(f: &BoltFailure) -> SprigError {
    let msg = f.message.clone();
    match f.code.as_str() {
        "Neo.ClientError.Statement.SyntaxError" => SprigError::new(SprigCode::GraphSyntax, msg),
        "Neo.ClientError.Database.DatabaseNotFound" => {
            SprigError::new(SprigCode::GraphNotFound, msg)
        }
        "Neo.ClientError.Statement.TypeError" => SprigError::new(SprigCode::GraphPropertyType, msg),
        "Neo.ClientError.Security.Forbidden" => SprigError::standard(1142, *b"42000", msg),
        "Neo.ClientError.Security.Unauthorized" => SprigError::standard(1045, *b"28000", msg),
        "Neo.TransientError.Transaction.DeadlockDetected" => {
            SprigError::standard(1213, *b"40001", msg)
        }
        "Neo.TransientError.Transaction.Terminated" => SprigError::standard(1317, *b"70100", msg),
        "Neo.TransientError.General.DatabaseUnavailable" => {
            SprigError::new(SprigCode::ShardMapChanged, msg)
        }
        "Neo.ClientError.Schema.ConstraintValidationFailed" if msg.contains("relationship") => {
            SprigError::new(SprigCode::GraphNodeHasRelationships, msg)
        }
        "Neo.ClientError.Schema.ConstraintValidationFailed" => {
            SprigError::standard(1062, *b"23000", msg)
        }
        code => {
            if let Some(name) = code.strip_prefix("Neo.ClientError.Statement.")
                && let Some(c) = SprigCode::ALL.iter().find(|c| c.mongo_code_name() == name)
            {
                return SprigError::new(*c, msg);
            }
            SprigError::new(
                SprigCode::LanguageStatementFailed,
                format!("[{code}] {msg}"),
            )
        }
    }
}

/// A column type from a value, so numbers align and print as numbers.
fn type_of(v: &PackValue) -> Option<&'static str> {
    match v {
        PackValue::Null => None,
        PackValue::Int(_) => Some("BIGINT"),
        PackValue::Float(_) => Some("DOUBLE"),
        PackValue::Bool(_) => Some("BOOLEAN"),
        PackValue::String(_) => Some("VARCHAR"),
        _ => Some("JSON"),
    }
}

/// A cell: text for scalars, JSON for the rest.
fn cell(v: &PackValue) -> Option<String> {
    match v {
        PackValue::Null => None,
        PackValue::String(s) => Some(s.clone()),
        PackValue::Int(i) => Some(i.to_string()),
        PackValue::Float(f) => Some(f.to_string()),
        PackValue::Bool(b) => Some(b.to_string()),
        other => Some(to_json(other).to_string()),
    }
}

/// A PackStream value as JSON: graph structures tagged (`{"$node"}`,
/// `{"$rel"}`, `{"$path"}`), other structures (temporal, spatial) as
/// `{"$struct": tag, "fields": [...]}`, bytes as `{"$bytes": hex}`. The
/// same shape as `sprig --protocol bolt` (crate `sprig-shell`).
fn to_json(v: &PackValue) -> Json {
    match v {
        PackValue::Null => Json::Null,
        PackValue::Bool(b) => Json::Bool(*b),
        PackValue::Int(i) => json!(i),
        PackValue::Float(f) => json!(f),
        PackValue::Bytes(b) => {
            let hex = b.iter().fold(String::new(), |mut acc, x| {
                let _ = write!(acc, "{x:02x}");
                acc
            });
            json!({ "$bytes": hex })
        }
        PackValue::String(s) => Json::String(s.clone()),
        PackValue::List(l) => Json::Array(l.iter().map(to_json).collect()),
        PackValue::Map(m) => Json::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), to_json(v)))
                .collect::<Map<_, _>>(),
        ),
        PackValue::Struct { tag, fields } => {
            let f = |i: usize| fields.get(i).map_or(Json::Null, to_json);
            match tag {
                0x4E => json!({"$node": {"id": f(0), "labels": f(1), "props": f(2)}}),
                0x52 => {
                    json!({"$rel": {"id": f(0), "start": f(1), "end": f(2), "type": f(3), "props": f(4)}})
                }
                0x72 => json!({"$rel": {"id": f(0), "type": f(1), "props": f(2)}}),
                0x50 => path(fields),
                _ => {
                    json!({"$struct": tag, "fields": fields.iter().map(to_json).collect::<Vec<_>>()})
                }
            }
        }
    }
}

/// A Bolt path (nodes, unbound relationships, indices) as an alternating
/// list of `{"$node"}` and `{"$rel"}` with start and end filled in.
fn path(fields: &[PackValue]) -> Json {
    let list = |i: usize| -> Vec<PackValue> {
        fields
            .get(i)
            .and_then(PackValue::as_list)
            .map(<[PackValue]>::to_vec)
            .unwrap_or_default()
    };
    let nodes: Vec<Json> = list(0).iter().map(to_json).collect();
    let rels: Vec<Json> = list(1).iter().map(to_json).collect();
    let idx: Vec<i64> = list(2).iter().filter_map(PackValue::as_int).collect();
    let node_id = |n: &Json| {
        n.get("$node")
            .and_then(|x| x.get("id"))
            .cloned()
            .unwrap_or(Json::Null)
    };
    let Some(first) = nodes.first() else {
        return json!({"$path": []});
    };
    let mut out = vec![first.clone()];
    let mut prev = first.clone();
    for pair in idx.chunks(2) {
        let (ri, ni) = (pair[0], pair.get(1).copied().unwrap_or(0));
        let node = nodes
            .get(usize::try_from(ni).unwrap_or(0))
            .cloned()
            .unwrap_or(Json::Null);
        let k = usize::try_from(ri.unsigned_abs())
            .unwrap_or(1)
            .saturating_sub(1);
        let mut rel = rels.get(k).cloned().unwrap_or(Json::Null);
        if let Some(r) = rel.get_mut("$rel").and_then(Json::as_object_mut) {
            let (s, e) = if ri > 0 {
                (node_id(&prev), node_id(&node))
            } else {
                (node_id(&node), node_id(&prev))
            };
            r.insert("start".into(), s);
            r.insert("end".into(), e);
        }
        out.push(rel);
        out.push(node.clone());
        prev = node;
    }
    json!({ "$path": out })
}

/// How long a test waits for a channel.
#[cfg(test)]
const WAIT: std::time::Duration = std::time::Duration::from_secs(5);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn languages_parse() {
        assert_eq!(Language::parse("").unwrap(), Language::Sql);
        assert_eq!(Language::parse("SQL").unwrap(), Language::Sql);
        assert_eq!(Language::parse("cypher").unwrap(), Language::Cypher);
        assert_eq!(Language::parse("Gql").unwrap(), Language::Gql);
        assert_eq!(Language::parse("mongo").unwrap(), Language::Mongo);
        let e = Language::parse("redis").unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::QueryLanguageInvalid));
    }

    #[test]
    fn outputs_split_and_check() {
        assert_eq!(with_output("mongo", ""), "mongo");
        assert_eq!(
            with_output("mongo", "canonical_ejson"),
            "mongo;output=canonical_ejson"
        );
        assert_eq!(split_output("mongo").unwrap(), ("mongo", false));
        assert_eq!(
            split_output("mongo;output=canonical_ejson").unwrap(),
            ("mongo", true)
        );
        assert_eq!(
            split_output("mongo;output=relaxed_ejson").unwrap(),
            ("mongo", false)
        );
        for bad in [
            "mongo;output=xml",
            "sql;output=canonical_ejson",
            "cypher;output=canonical_ejson",
        ] {
            let e = split_output(bad).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::QueryLanguageInvalid), "{bad}");
        }
        // Canonical keeps int32, int64 and double apart; relaxed doesn't.
        let d = Bson::Document(doc! {"i": 1_i32, "l": 2_i64, "f": 3.0_f64});
        assert_eq!(
            ext_json(d.clone(), true),
            r#"{"i":{"$numberInt":"1"},"l":{"$numberLong":"2"},"f":{"$numberDouble":"3.0"}}"#
        );
        assert_eq!(ext_json(d, false), r#"{"i":1,"l":2,"f":3.0}"#);
    }

    #[test]
    fn mongo_commands_are_json_documents() {
        let d = command_doc(
            r#"{"find": "users", "filter": {"_id": {"$oid": "65a000000000000000000001"}}}"#,
        )
        .unwrap();
        assert_eq!(d.keys().next().map(String::as_str), Some("find"));
        assert!(matches!(
            d.get_document("filter").unwrap().get("_id"),
            Some(Bson::ObjectId(_))
        ));
        for bad in ["[1]", "{}", "db.users.find()", "\"find\""] {
            let e = command_doc(bad).unwrap_err();
            assert_eq!(e.code(), Some(SprigCode::QueryLanguageInvalid), "{bad}");
        }
    }

    #[test]
    fn mongo_errors_keep_their_meaning() {
        let e = mongo_error(&doc! {"code": 11000, "codeName": "DuplicateKey", "errmsg": "dup"});
        assert_eq!(e.protocol().mysql_errno, 1062);
        let e = mongo_error(&doc! {"code": 13, "codeName": "Unauthorized", "errmsg": "no"});
        assert_eq!(e.protocol().mysql_errno, 1142);
        let e = mongo_error(&doc! {"code": 39902, "errmsg": "x"});
        assert_eq!(e.code(), Some(SprigCode::GraphNotFound));
        let e = mongo_error(&doc! {"code": 2, "codeName": "BadValue", "errmsg": "bad"});
        assert_eq!(e.code(), Some(SprigCode::LanguageStatementFailed));
        assert!(
            e.message.contains("MongoDB error 2 BadValue"),
            "{}",
            e.message
        );
        // ok: 1 with a write error is the write error.
        let r =
            doc! {"ok": 1.0, "n": 0, "writeErrors": [{"index": 0, "code": 11000, "errmsg": "dup"}]};
        assert_eq!(check_reply(&r).unwrap_err().protocol().mysql_errno, 1062);
        assert!(check_reply(&doc! {"ok": 1.0, "n": 1}).is_ok());
    }

    #[test]
    fn bolt_failures_map_back() {
        let f = |c: &str| from_failure(&BoltFailure::new(c, "m"));
        assert_eq!(
            f("Neo.ClientError.Statement.SyntaxError").code(),
            Some(SprigCode::GraphSyntax)
        );
        assert_eq!(
            f("Neo.ClientError.Statement.SprigGraphBudgetExceeded").code(),
            Some(SprigCode::GraphBudgetExceeded)
        );
        assert_eq!(
            f("Neo.ClientError.Security.Forbidden")
                .protocol()
                .mysql_errno,
            1142
        );
        let e = f("Neo.ClientError.Statement.ParameterMissing");
        assert_eq!(e.code(), Some(SprigCode::LanguageStatementFailed));
        assert!(
            e.message
                .starts_with("[Neo.ClientError.Statement.ParameterMissing]")
        );
        // Each code the Bolt front end gives for a SprigDB code comes back.
        for c in [
            SprigCode::GraphSyntax,
            SprigCode::GraphNotFound,
            SprigCode::GraphBudgetExceeded,
            SprigCode::GraphPropertyType,
            SprigCode::GraphUnsupported,
        ] {
            let e = SprigError::new(c, "x");
            let back = from_failure(&BoltFailure::new(
                sprig_graph::neo4j::status_code(&e),
                sprig_graph::neo4j::message(&e),
            ));
            assert_eq!(back.code(), Some(c));
        }
    }

    #[test]
    fn graph_cells_are_text_or_json() {
        assert_eq!(cell(&PackValue::Int(7)).as_deref(), Some("7"));
        assert_eq!(cell(&PackValue::str("x")).as_deref(), Some("x"));
        assert_eq!(cell(&PackValue::Null), None);
        let node = PackValue::Struct {
            tag: 0x4E,
            fields: vec![
                PackValue::Int(1),
                PackValue::List(vec![PackValue::str("P")]),
                PackValue::map([("name", PackValue::str("Asha"))]),
                PackValue::str("n:1"),
            ],
        };
        assert_eq!(
            cell(&node).as_deref(),
            Some(r#"{"$node":{"id":1,"labels":["P"],"props":{"name":"Asha"}}}"#)
        );
        assert_eq!(type_of(&node), Some("JSON"));
        assert_eq!(type_of(&PackValue::Float(1.5)), Some("DOUBLE"));
    }

    #[tokio::test]
    async fn caps_truncate_and_a_gone_reader_is_seen() {
        let (tx, mut rx) = mpsc::channel(64);
        let mut out = Out::new(
            &tx,
            2,
            Caps {
                max_rows: 3,
                ..Caps::default()
            },
        );
        out.header(vec![column("n", "BIGINT")]).await.ok().unwrap();
        for i in 0..5 {
            let kept = out.row(vec![Some(i.to_string())]).await.ok().unwrap();
            assert_eq!(kept, i < 3);
        }
        out.end(0, String::new()).await.ok().unwrap();
        drop(tx);
        let mut rows = 0;
        let mut truncated = false;
        while let Ok(Some(item)) = tokio::time::timeout(WAIT, rx.recv()).await {
            if let Item::Part(QueryResponse { part: Some(p) }) = item {
                match p {
                    query_response::Part::Rows(b) => rows += b.rows.len(),
                    query_response::Part::End(e) => truncated = e.truncated,
                    query_response::Part::Header(_) => {}
                }
            }
        }
        assert_eq!((rows, truncated), (3, true));
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let out = Out::new(&tx, 2, Caps::default());
        assert!(out.header(Vec::new()).await.is_err());
    }
}
