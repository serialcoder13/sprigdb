//! The native MongoDB client (`--protocol mongo`): `OP_MSG` and
//! SCRAM-SHA-256 from `sprig-proto-mongo`, and a parser for the `mongosh`
//! subset Studio accepts: a call chain rooted at `db`,
//! literals, and the helpers `ObjectId`, `ISODate`, `NumberLong`,
//! `NumberInt`, `NumberDecimal`. Input is parsed, never evaluated.

use base64::Engine as _;
use bson::{Bson, Document, RawDocumentBuf, doc};
use serde_json::{Map, Value, json};
use sprig_proto_mongo::codec::{Frame, OpMsg, decode, encode};
use sprig_proto_mongo::scram::ScramClient;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Result, ShellCode, ShellError};
use crate::model::{EndInfo, ResultEvent, Sink};
use crate::native::{NativeTarget, split_addr};

fn perr(m: impl Into<String>) -> ShellError {
    ShellError::shell(ShellCode::Parse, m.into())
}

/// A parser of `mongosh` literals and call chains.
struct P<'a> {
    s: &'a [char],
    i: usize,
}

impl P<'_> {
    fn ws(&mut self) {
        while self.i < self.s.len() {
            if self.s[self.i].is_whitespace() {
                self.i += 1;
            } else if self.s[self.i] == '/' && self.s.get(self.i + 1) == Some(&'/') {
                while self.i < self.s.len() && self.s[self.i] != '\n' {
                    self.i += 1;
                }
            } else {
                break;
            }
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.ws();
        self.s.get(self.i).copied()
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, c: char) -> Result<()> {
        if self.eat(c) {
            Ok(())
        } else {
            Err(perr(format!("expected '{c}' at character {}", self.i + 1)))
        }
    }

    fn ident(&mut self) -> Option<String> {
        self.ws();
        let start = self.i;
        while self.i < self.s.len()
            && (self.s[self.i].is_alphanumeric() || matches!(self.s[self.i], '_' | '$'))
        {
            self.i += 1;
        }
        (self.i > start).then(|| self.s[start..self.i].iter().collect())
    }

    fn string(&mut self) -> Result<String> {
        let q = self.s[self.i];
        self.i += 1;
        let mut out = String::new();
        while self.i < self.s.len() {
            let c = self.s[self.i];
            self.i += 1;
            if c == q {
                return Ok(out);
            }
            if c == '\\' {
                let e = self.s.get(self.i).copied().unwrap_or('\\');
                self.i += 1;
                out.push(match e {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    o => o,
                });
            } else {
                out.push(c);
            }
        }
        Err(perr("open string"))
    }

    fn number(&mut self) -> Result<Value> {
        let start = self.i;
        while self.i < self.s.len()
            && (self.s[self.i].is_ascii_digit()
                || matches!(self.s[self.i], '-' | '+' | '.' | 'e' | 'E'))
        {
            self.i += 1;
        }
        let t: String = self.s[start..self.i].iter().collect();
        if let Ok(i) = t.parse::<i64>() {
            return Ok(if i32::try_from(i).is_ok() {
                json!(i)
            } else {
                json!({"$numberLong": t})
            });
        }
        t.parse::<f64>()
            .map(|f| json!(f))
            .map_err(|_| perr(format!("bad number {t}")))
    }

    fn args(&mut self) -> Result<Vec<Value>> {
        self.expect('(')?;
        let mut out = Vec::new();
        if self.eat(')') {
            return Ok(out);
        }
        loop {
            out.push(self.value()?);
            if self.eat(')') {
                return Ok(out);
            }
            self.expect(',')?;
            if self.eat(')') {
                return Ok(out);
            }
        }
    }

    fn value(&mut self) -> Result<Value> {
        match self.peek() {
            Some('{') => {
                self.i += 1;
                let mut m = Map::new();
                loop {
                    if self.eat('}') {
                        return Ok(Value::Object(m));
                    }
                    let key = match self.peek() {
                        Some('"' | '\'') => self.string()?,
                        _ => self.ident().ok_or_else(|| {
                            perr(format!("expected a key at character {}", self.i + 1))
                        })?,
                    };
                    self.expect(':')?;
                    let v = self.value()?;
                    m.insert(key, v);
                    if !self.eat(',') {
                        self.expect('}')?;
                        return Ok(Value::Object(m));
                    }
                }
            }
            Some('[') => {
                self.i += 1;
                let mut a = Vec::new();
                loop {
                    if self.eat(']') {
                        return Ok(Value::Array(a));
                    }
                    a.push(self.value()?);
                    if !self.eat(',') {
                        self.expect(']')?;
                        return Ok(Value::Array(a));
                    }
                }
            }
            Some('"' | '\'') => self.string().map(Value::String),
            Some(c) if c.is_ascii_digit() || c == '-' || c == '.' => self.number(),
            Some('/') => {
                self.i += 1;
                let mut pat = String::new();
                while self.i < self.s.len() && self.s[self.i] != '/' {
                    if self.s[self.i] == '\\' && self.i + 1 < self.s.len() {
                        pat.push('\\');
                        self.i += 1;
                    }
                    pat.push(self.s[self.i]);
                    self.i += 1;
                }
                self.expect('/')?;
                let opts = self.ident().unwrap_or_default();
                Ok(json!({"$regularExpression": {"pattern": pat, "options": opts}}))
            }
            Some(_) => {
                let w = self
                    .ident()
                    .ok_or_else(|| perr(format!("unexpected character at {}", self.i + 1)))?;
                match w.as_str() {
                    "true" => Ok(Value::Bool(true)),
                    "false" => Ok(Value::Bool(false)),
                    "null" | "undefined" => Ok(Value::Null),
                    "new" => self.value(),
                    "ObjectId" => {
                        let a = self.args()?;
                        Ok(json!({"$oid": a.first().and_then(Value::as_str).unwrap_or_default()}))
                    }
                    "ISODate" | "Date" => {
                        let a = self.args()?;
                        Ok(
                            json!({"$date": a.first().and_then(Value::as_str).unwrap_or("1970-01-01T00:00:00Z")}),
                        )
                    }
                    "NumberLong" | "NumberInt" | "NumberDecimal" => {
                        let a = self.args()?;
                        let t = match a.first() {
                            Some(Value::String(s)) => s.clone(),
                            Some(v) => v.to_string(),
                            None => "0".to_owned(),
                        };
                        let k = match w.as_str() {
                            "NumberLong" => "$numberLong",
                            "NumberInt" => "$numberInt",
                            _ => "$numberDecimal",
                        };
                        Ok(json!({ k: t }))
                    }
                    other => Err(perr(format!(
                        "{other}: only literals and ObjectId, ISODate, NumberLong, NumberInt and NumberDecimal are accepted (input is never run as code)"
                    ))),
                }
            }
            None => Err(perr("unexpected end of input")),
        }
    }
}

/// A parsed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Call {
    /// `use db`.
    Use(String),
    /// `show dbs`.
    ShowDbs,
    /// `show collections`.
    ShowCollections,
    /// `db.method(args)`.
    Db(String, Vec<Value>),
    /// `db.coll.method(args).chain(args)...`.
    Coll(String, String, Vec<Value>, Vec<(String, Vec<Value>)>),
}

/// Parses one statement.
///
/// # Errors
/// 39985 for anything outside the subset.
pub fn parse_call(stmt: &str) -> Result<Call> {
    let t = stmt.trim().trim_end_matches(';').trim();
    let words: Vec<&str> = t.split_whitespace().collect();
    match words.as_slice() {
        ["use", db] => return Ok(Call::Use((*db).to_owned())),
        ["show", "dbs" | "databases"] => return Ok(Call::ShowDbs),
        ["show", "collections" | "tables"] => return Ok(Call::ShowCollections),
        _ => {}
    }
    let chars: Vec<char> = t.chars().collect();
    let mut p = P { s: &chars, i: 0 };
    if p.ident().as_deref() != Some("db") {
        return Err(perr("a statement starts with db., use, or show"));
    }
    p.expect('.')?;
    let first = p
        .ident()
        .ok_or_else(|| perr("expected a collection or method after db."))?;
    let (coll, method) = if first == "getCollection" {
        let a = p.args()?;
        let c = a
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| perr("getCollection takes a name"))?
            .to_owned();
        p.expect('.')?;
        (c, p.ident().ok_or_else(|| perr("expected a method"))?)
    } else if p.peek() == Some('(') {
        let a = p.args()?;
        if p.peek().is_some() {
            return Err(perr("unexpected text after the call"));
        }
        return Ok(Call::Db(first, a));
    } else {
        p.expect('.')?;
        (first, p.ident().ok_or_else(|| perr("expected a method"))?)
    };
    let args = p.args()?;
    let mut chain = Vec::new();
    while p.eat('.') {
        let m = p.ident().ok_or_else(|| perr("expected a method"))?;
        let a = p.args()?;
        chain.push((m, a));
    }
    if p.peek().is_some() {
        return Err(perr(format!("unexpected text at character {}", p.i + 1)));
    }
    Ok(Call::Coll(coll, method, args, chain))
}

fn bson_of(v: &Value) -> Result<Bson> {
    Bson::try_from(v.clone()).map_err(|e| perr(format!("{e}")))
}

fn doc_of(v: Option<&Value>) -> Result<Document> {
    match v {
        None | Some(Value::Null) => Ok(Document::new()),
        Some(v) => match bson_of(v)? {
            Bson::Document(d) => Ok(d),
            other => Err(perr(format!("expected a document, got {other}"))),
        },
    }
}

/// The command a call becomes, and whether its reply is a cursor.
///
/// # Errors
/// 39985 for an unknown method or bad arguments.
#[allow(clippy::too_many_lines)]
pub fn command(call: &Call) -> Result<(Document, bool)> {
    match call {
        Call::Use(_) => Ok((doc! {"ping": 1}, false)),
        Call::ShowDbs => Ok((doc! {"listDatabases": 1, "nameOnly": true}, false)),
        Call::ShowCollections => Ok((
            doc! {"listCollections": 1, "nameOnly": true, "cursor": {}},
            true,
        )),
        Call::Db(m, a) => match m.as_str() {
            "runCommand" | "adminCommand" => Ok((doc_of(a.first())?, false)),
            "getCollectionNames" => Ok((
                doc! {"listCollections": 1, "nameOnly": true, "cursor": {}},
                true,
            )),
            "dropDatabase" => Ok((doc! {"dropDatabase": 1}, false)),
            "stats" => Ok((doc! {"dbStats": 1}, false)),
            "createCollection" => Ok((
                doc! {"create": a.first().and_then(Value::as_str).unwrap_or_default()},
                false,
            )),
            other => Err(perr(format!("db.{other}() isn't supported"))),
        },
        Call::Coll(c, m, a, chain) => {
            let c = c.as_str();
            let mut cmd = match m.as_str() {
                "find" | "findOne" => {
                    let mut d = doc! {"find": c, "filter": doc_of(a.first())?};
                    if let Some(p) = a.get(1) {
                        d.insert("projection", doc_of(Some(p))?);
                    }
                    if m == "findOne" {
                        d.insert("limit", 1);
                        d.insert("singleBatch", true);
                    }
                    d
                }
                "aggregate" => {
                    let pipeline = match a.first() {
                        Some(v) => bson_of(v)?,
                        None => Bson::Array(Vec::new()),
                    };
                    doc! {"aggregate": c, "pipeline": pipeline, "cursor": {}}
                }
                "countDocuments" => doc! {
                    "aggregate": c,
                    "pipeline": [{"$match": doc_of(a.first())?}, {"$group": {"_id": 1, "n": {"$sum": 1}}}],
                    "cursor": {},
                },
                "insertOne" => doc! {"insert": c, "documents": [doc_of(a.first())?]},
                "insertMany" => {
                    let docs = match a.first() {
                        Some(v) => bson_of(v)?,
                        None => Bson::Array(Vec::new()),
                    };
                    doc! {"insert": c, "documents": docs}
                }
                "updateOne" | "updateMany" | "replaceOne" => {
                    let upsert = a
                        .get(2)
                        .and_then(|o| o.get("upsert"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let u = match a.get(1) {
                        Some(v) => bson_of(v)?,
                        None => return Err(perr(format!("{m} needs a filter and an update"))),
                    };
                    doc! {"update": c, "updates": [{"q": doc_of(a.first())?, "u": u, "multi": m == "updateMany", "upsert": upsert}]}
                }
                "deleteOne" | "deleteMany" => {
                    doc! {"delete": c, "deletes": [{"q": doc_of(a.first())?, "limit": i32::from(m == "deleteOne")}]}
                }
                "distinct" => {
                    let key = a
                        .first()
                        .and_then(Value::as_str)
                        .ok_or_else(|| perr("distinct takes a field name"))?;
                    doc! {"distinct": c, "key": key, "query": doc_of(a.get(1))?}
                }
                "createIndex" => {
                    let keys = doc_of(a.first())?;
                    let name = a
                        .get(1)
                        .and_then(|o| o.get("name"))
                        .and_then(Value::as_str)
                        .map_or_else(
                            || {
                                keys.iter()
                                    .map(|(k, v)| format!("{k}_{v}"))
                                    .collect::<Vec<_>>()
                                    .join("_")
                            },
                            str::to_owned,
                        );
                    doc! {"createIndexes": c, "indexes": [{"key": keys, "name": name}]}
                }
                "getIndexes" => doc! {"listIndexes": c, "cursor": {}},
                "drop" => doc! {"drop": c},
                other => return Err(perr(format!("{other}() isn't supported"))),
            };
            for (cm, ca) in chain {
                match cm.as_str() {
                    "sort" => {
                        cmd.insert("sort", doc_of(ca.first())?);
                    }
                    "limit" => {
                        cmd.insert("limit", ca.first().and_then(Value::as_i64).unwrap_or(0));
                    }
                    "skip" => {
                        cmd.insert("skip", ca.first().and_then(Value::as_i64).unwrap_or(0));
                    }
                    "projection" => {
                        cmd.insert("projection", doc_of(ca.first())?);
                    }
                    "toArray" | "pretty" => {}
                    other => return Err(perr(format!(".{other}() isn't supported in a chain"))),
                }
            }
            let cursor = cmd.contains_key("find")
                || cmd.contains_key("aggregate")
                || cmd.contains_key("listIndexes");
            Ok((cmd, cursor))
        }
    }
}

/// A MongoDB connection.
pub struct MongoBackend {
    target: NativeTarget,
    stream: Option<TcpStream>,
    db: String,
    next_id: i32,
}

fn io(e: impl std::fmt::Display) -> ShellError {
    ShellError::shell(ShellCode::Unreachable, e.to_string())
}

impl MongoBackend {
    async fn call(&mut self, mut cmd: Document, db: &str) -> Result<Document> {
        cmd.insert("$db", db);
        let body = RawDocumentBuf::from_document(&cmd).map_err(io)?;
        self.next_id += 1;
        let msg = encode(
            self.next_id,
            0,
            &Frame::Msg(OpMsg {
                flags: 0,
                body,
                sequences: Vec::new(),
            }),
            None,
        )
        .map_err(|e| io(format!("{e:?}")))?;
        let s = self.stream.as_mut().ok_or_else(|| io("not connected"))?;
        s.write_all(&msg).await.map_err(io)?;
        let mut head = [0u8; 16];
        s.read_exact(&mut head).await.map_err(io)?;
        let len = usize::try_from(i32::from_le_bytes([head[0], head[1], head[2], head[3]]))
            .map_err(io)?;
        if !(16..=48 << 20).contains(&len) {
            return Err(io(format!("reply of {len} bytes")));
        }
        let mut whole = head.to_vec();
        whole.resize(len, 0);
        s.read_exact(&mut whole[16..]).await.map_err(io)?;
        let (_, frame, _) = decode(&whole).map_err(|e| io(format!("{e:?}")))?;
        let reply = match frame {
            Frame::Msg(m) => m.body.to_document().map_err(io)?,
            Frame::Reply(r) => r
                .docs
                .first()
                .map(bson::RawDocumentBuf::to_document)
                .transpose()
                .map_err(io)?
                .unwrap_or_default(),
            Frame::Query(_) => return Err(io("unexpected OP_QUERY")),
        };
        let ok = match reply.get("ok") {
            Some(Bson::Double(f)) => (*f - 1.0).abs() < f64::EPSILON,
            Some(Bson::Int32(i)) => *i == 1,
            Some(Bson::Int64(i)) => *i == 1,
            Some(Bson::Boolean(b)) => *b,
            _ => false,
        };
        if !ok {
            let code = match reply.get("code") {
                Some(Bson::Int32(i)) => u32::try_from(*i).unwrap_or(0),
                Some(Bson::Int64(i)) => u32::try_from(*i).unwrap_or(0),
                Some(Bson::Double(f)) => format!("{f}").parse().unwrap_or(0),
                _ => 0,
            };
            let name = reply.get_str("codeName").unwrap_or_default().to_owned();
            let msg = reply
                .get_str("errmsg")
                .unwrap_or("command failed")
                .to_owned();
            return Err(ShellError::server(code, name, msg));
        }
        Ok(reply)
    }

    async fn open(&mut self) -> Result<()> {
        let (host, port) = split_addr(&self.target.addr, 27017)?;
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| io(format!("{}: {e}", self.target.addr)))?;
        let _ = tcp.set_nodelay(true);
        self.stream = Some(tcp);
        self.call(
            doc! {"hello": 1, "client": {"application": {"name": "sprig"}}},
            "admin",
        )
        .await?;
        if self.target.password.is_empty() {
            return Ok(());
        }
        let mut nonce = [0u8; 24];
        getrandom::fill(&mut nonce).map_err(io)?;
        let cnonce = base64::engine::general_purpose::STANDARD.encode(nonce);
        let mut sc = ScramClient::new(&self.target.user, self.target.password.as_bytes(), &cnonce);
        let bin = |b: Vec<u8>| {
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: b,
            })
        };
        let auth_err = |e: ShellError| {
            let mut e = e;
            if e.code == 18 || e.code == 0 {
                e.code = 1045;
            }
            e
        };
        let r = self
            .call(
                doc! {"saslStart": 1, "mechanism": "SCRAM-SHA-256", "payload": bin(sc.first()), "options": {"skipEmptyExchange": true}},
                "admin",
            )
            .await
            .map_err(auth_err)?;
        let conv = r.get("conversationId").cloned().unwrap_or(Bson::Int32(1));
        let server_first = r.get_binary_generic("payload").map_err(io)?.clone();
        let fin = sc
            .finish(&server_first)
            .map_err(|e| io(format!("scram: {e:?}")))?;
        let mut r = self
            .call(
                doc! {"saslContinue": 1, "conversationId": conv.clone(), "payload": bin(fin)},
                "admin",
            )
            .await
            .map_err(auth_err)?;
        let server_final = r.get_binary_generic("payload").map_err(io)?.clone();
        if !sc.verify(&server_final) {
            return Err(ShellError::server(
                1045,
                "",
                "the server's SCRAM signature didn't match",
            ));
        }
        while !r.get_bool("done").unwrap_or(true) {
            r = self
                .call(doc! {"saslContinue": 1, "conversationId": conv.clone(), "payload": bin(Vec::new())}, "admin")
                .await
                .map_err(auth_err)?;
        }
        Ok(())
    }

    /// Connects, says hello and authenticates with SCRAM-SHA-256.
    ///
    /// # Errors
    /// 39987 or the server's refusal.
    pub async fn connect(target: NativeTarget) -> Result<Self> {
        let db = target.database.clone().unwrap_or_else(|| "test".to_owned());
        let mut m = Self {
            target,
            stream: None,
            db,
            next_id: 0,
        };
        m.open().await?;
        Ok(m)
    }

    /// The address.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.target.addr
    }

    pub(crate) async fn reconnect(&mut self) -> Result<()> {
        self.stream = None;
        self.open().await
    }

    #[allow(clippy::similar_names)]
    pub(crate) async fn run(&mut self, stmt: &str, sink: &mut dyn Sink) -> Result<()> {
        if self.stream.is_none() {
            self.open().await?;
        }
        let call = parse_call(stmt)?;
        let ioe = |e: std::io::Error| ShellError::shell(ShellCode::Cancelled, e.to_string());
        if let Call::Use(db) = &call {
            self.db.clone_from(db);
            return sink
                .event(ResultEvent::End(EndInfo {
                    info: format!("switched to db {db}"),
                    ..EndInfo::default()
                }))
                .map_err(ioe);
        }
        let (cmd, cursor) = command(&call)?;
        let coll = cmd
            .iter()
            .next()
            .map(|(_, v)| v.as_str().unwrap_or_default().to_owned())
            .unwrap_or_default();
        let db = if matches!(call, Call::ShowDbs) {
            "admin".to_owned()
        } else {
            self.db.clone()
        };
        let reply = self.call(cmd, &db).await?;
        let emit = |sink: &mut dyn Sink, d: Bson| {
            sink.event(ResultEvent::Doc(d.into_relaxed_extjson()))
                .map_err(ioe)
        };
        if cursor {
            let mut c = reply.get_document("cursor").cloned().unwrap_or_default();
            let mut batch = c.get_array("firstBatch").cloned().unwrap_or_default();
            loop {
                for d in batch {
                    emit(sink, d)?;
                }
                let id = c.get_i64("id").unwrap_or(0);
                if id == 0 {
                    break;
                }
                let r = self
                    .call(doc! {"getMore": id, "collection": coll.clone()}, &db)
                    .await?;
                c = r.get_document("cursor").cloned().unwrap_or_default();
                batch = c.get_array("nextBatch").cloned().unwrap_or_default();
            }
        } else if let Call::ShowDbs = call {
            for d in reply.get_array("databases").cloned().unwrap_or_default() {
                emit(sink, d)?;
            }
        } else {
            let mut r = reply;
            r.remove("ok");
            r.remove("$clusterTime");
            r.remove("operationTime");
            emit(sink, Bson::Document(r))?;
        }
        sink.event(ResultEvent::End(EndInfo::default()))
            .map_err(ioe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_subset() {
        let c = parse_call("db.orders.find({status: 'paid', _id: ObjectId('65a1f0aa65a1f0aa65a1f0aa'), n: NumberLong(7), at: {$gte: ISODate(\"2026-01-01T00:00:00Z\")}}, {_id: 0},).sort({at: -1}).limit(5)")
            .expect("parse");
        let (cmd, cursor) = command(&c).expect("command");
        assert!(cursor);
        assert_eq!(cmd.get_str("find").expect("find"), "orders");
        assert_eq!(cmd.get_i64("limit").expect("limit"), 5);
        let f = cmd.get_document("filter").expect("filter");
        assert!(matches!(f.get("_id"), Some(Bson::ObjectId(_))));
        assert!(matches!(f.get("n"), Some(Bson::Int32(_) | Bson::Int64(_))));
        assert!(matches!(
            f.get_document("at").expect("at").get("$gte"),
            Some(Bson::DateTime(_))
        ));
        assert_eq!(
            parse_call("use shop").expect("use"),
            Call::Use("shop".into())
        );
        let (d, _) = command(&parse_call("db.t.deleteMany({})").expect("p")).expect("c");
        assert_eq!(d.get_array("deletes").expect("deletes").len(), 1);
        let (i, _) = command(
            &parse_call("db.getCollection('a b').insertMany([{x: 1.5}, {x: -2}])").expect("p"),
        )
        .expect("c");
        assert_eq!(i.get_str("insert").expect("insert"), "a b");
    }

    #[test]
    fn refuses_code() {
        assert_eq!(
            parse_call("db.t.find({$where: function() { return 1 }})")
                .expect_err("code")
                .code,
            39985
        );
        assert!(parse_call("var x = 1").is_err());
        assert!(parse_call("db.t.find({}).forEach(printjson)").is_err());
        assert!(parse_call("db.t.find({a: 'open").is_err());
    }
}
