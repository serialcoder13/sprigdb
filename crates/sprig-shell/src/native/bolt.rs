//! The native Bolt client (`--protocol bolt`): the workspace's own
//! `sprig_proto_bolt::client::BoltClient`. Results come back as rows whose
//! cells are text, or JSON for nodes, relationships and paths
//! (`{"$node"}`, `{"$rel"}`, `{"$path"}`), which the graph view draws.
//! `:begin`, `:commit` and `:rollback` (or `BEGIN`, `COMMIT`, `ROLLBACK`)
//! manage an explicit transaction, as in `cypher-shell`.

use std::fmt::Write as _;

use serde_json::{Map, Value, json};
use sprig_proto_bolt::client::{BoltClient, ClientError};
use sprig_proto_bolt::packstream::PackValue;
use tokio::net::TcpStream;

use crate::error::{Result, ShellCode, ShellError};
use crate::model::{Column, EndInfo, ResultEvent, Sink};
use crate::native::{NativeTarget, split_addr};

/// A Bolt connection.
pub struct BoltBackend {
    target: NativeTarget,
    client: Option<BoltClient<TcpStream>>,
    in_txn: bool,
}

fn err(e: ClientError) -> ShellError {
    if e.code.is_empty() {
        ShellError::shell(ShellCode::Unreachable, e.message)
    } else {
        // Neo4j codes like Neo.ClientError.Statement.SyntaxError; SprigDB
        // codes arrive in the message as "<number> ...".
        let n = e
            .message
            .split_whitespace()
            .find_map(|w| {
                w.trim_end_matches(':')
                    .parse::<u32>()
                    .ok()
                    .filter(|n| (1000..40000).contains(n))
            })
            .unwrap_or(0);
        ShellError::server(n, e.code, e.message)
    }
}

/// A PackStream value as JSON: graph structures tagged, temporal and
/// spatial values as `{"$struct": tag, "fields": [...]}`.
#[must_use]
pub fn to_json(v: &PackValue) -> Value {
    match v {
        PackValue::Null => Value::Null,
        PackValue::Bool(b) => Value::Bool(*b),
        PackValue::Int(i) => json!(i),
        PackValue::Float(f) => json!(f),
        PackValue::Bytes(b) => {
            json!({ "$bytes": b.iter().fold(String::new(), |mut acc, x| {
                let _ = write!(acc, "{x:02x}");
                acc
            }) })
        }
        PackValue::String(s) => Value::String(s.clone()),
        PackValue::List(l) => Value::Array(l.iter().map(to_json).collect()),
        PackValue::Map(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), to_json(v)))
                .collect::<Map<_, _>>(),
        ),
        PackValue::Struct { tag, fields } => {
            let f = |i: usize| fields.get(i).map_or(Value::Null, to_json);
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
fn path(fields: &[PackValue]) -> Value {
    let nodes: Vec<Value> = fields
        .first()
        .and_then(PackValue::as_list)
        .unwrap_or_default()
        .iter()
        .map(to_json)
        .collect();
    let rels: Vec<Value> = fields
        .get(1)
        .and_then(PackValue::as_list)
        .unwrap_or_default()
        .iter()
        .map(to_json)
        .collect();
    let idx: Vec<i64> = fields
        .get(2)
        .and_then(PackValue::as_list)
        .unwrap_or_default()
        .iter()
        .filter_map(PackValue::as_int)
        .collect();
    let node_id = |n: &Value| {
        n.get("$node")
            .and_then(|x| x.get("id"))
            .cloned()
            .unwrap_or(Value::Null)
    };
    let mut out = Vec::new();
    let Some(first) = nodes.first() else {
        return json!({"$path": []});
    };
    out.push(first.clone());
    let mut prev = first.clone();
    for pair in idx.chunks(2) {
        let (ri, ni) = (pair[0], pair.get(1).copied().unwrap_or(0));
        let node = nodes
            .get(usize::try_from(ni).unwrap_or(0))
            .cloned()
            .unwrap_or(Value::Null);
        let k = usize::try_from(ri.unsigned_abs())
            .unwrap_or(1)
            .saturating_sub(1);
        let mut rel = rels.get(k).cloned().unwrap_or(Value::Null);
        if let Some(r) = rel.get_mut("$rel").and_then(Value::as_object_mut) {
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

impl BoltBackend {
    async fn open(t: &NativeTarget) -> Result<BoltClient<TcpStream>> {
        let (host, port) = split_addr(&t.addr, 7687)?;
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| ShellError::shell(ShellCode::Unreachable, format!("{}: {e}", t.addr)))?;
        let _ = tcp.set_nodelay(true);
        let mut c = BoltClient::handshake(tcp, None).await.map_err(err)?;
        c.login(&t.user, &t.password).await.map_err(err)?;
        Ok(c)
    }

    /// Connects and logs in.
    ///
    /// # Errors
    /// 39987 or the server's refusal.
    pub async fn connect(target: NativeTarget) -> Result<Self> {
        let client = Some(Self::open(&target).await?);
        Ok(Self {
            target,
            client,
            in_txn: false,
        })
    }

    /// The address.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.target.addr
    }

    /// Whether a transaction is open.
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.in_txn
    }

    pub(crate) async fn reconnect(&mut self) -> Result<()> {
        self.client = None;
        self.in_txn = false;
        self.client = Some(Self::open(&self.target).await?);
        Ok(())
    }

    fn extra(&self) -> Vec<(&'static str, PackValue)> {
        match &self.target.database {
            Some(d) if !d.is_empty() => vec![("db", PackValue::str(d.clone()))],
            _ => Vec::new(),
        }
    }

    pub(crate) async fn run(&mut self, stmt: &str, sink: &mut dyn Sink) -> Result<()> {
        if self.client.is_none() {
            self.reconnect().await?;
        }
        let extra = self.extra();
        let c = self
            .client
            .as_mut()
            .ok_or_else(|| ShellError::shell(ShellCode::Unreachable, "not connected"))?;
        let io = |e: std::io::Error| ShellError::shell(ShellCode::Cancelled, e.to_string());
        let word = stmt
            .trim()
            .trim_start_matches(':')
            .trim_end_matches(';')
            .to_ascii_lowercase();
        let done = |sink: &mut dyn Sink, info: &str| {
            sink.event(ResultEvent::End(EndInfo {
                info: info.to_owned(),
                ..EndInfo::default()
            }))
            .map_err(io)
        };
        match word.as_str() {
            "begin" => {
                c.begin(extra).await.map_err(err)?;
                self.in_txn = true;
                return done(sink, "Transaction started");
            }
            "commit" => {
                let r = c.commit().await;
                self.in_txn = false;
                r.map_err(err)?;
                return done(sink, "Committed");
            }
            "rollback" => {
                let r = c.rollback().await;
                self.in_txn = false;
                r.map_err(err)?;
                return done(sink, "Rolled back");
            }
            _ => {}
        }
        let extra = if self.in_txn { Vec::new() } else { extra };
        let rows = c.query(stmt, Vec::new(), extra).await.map_err(err)?;
        if !rows.fields.is_empty() {
            sink.event(ResultEvent::Header(
                rows.fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| Column {
                        name: f.clone(),
                        type_name: rows
                            .records
                            .iter()
                            .find_map(|r| r.get(i).and_then(type_of))
                            .unwrap_or_default()
                            .to_owned(),
                        binary: false,
                    })
                    .collect(),
            ))
            .map_err(io)?;
            let data: Vec<Vec<Option<String>>> = rows
                .records
                .iter()
                .map(|r| r.iter().map(cell).collect())
                .collect();
            sink.event(ResultEvent::Rows(data)).map_err(io)?;
        }
        // Counters from the summary: "nodes-created" and the like.
        let counters = rows
            .summary
            .get("stats")
            .and_then(PackValue::as_map)
            .map(|m| {
                m.iter()
                    .map(|(k, v)| format!("{k}: {}", cell(v).unwrap_or_default()))
                    .collect::<Vec<_>>()
                    .join(", ")
            });
        done(sink, &counters.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: i64, label: &str) -> PackValue {
        PackValue::Struct {
            tag: 0x4E,
            fields: vec![
                PackValue::Int(id),
                PackValue::List(vec![PackValue::str(label)]),
                PackValue::map([("name", PackValue::str(format!("n{id}")))]),
                PackValue::str(id.to_string()),
            ],
        }
    }

    #[test]
    fn paths_as_json() {
        let rel = PackValue::Struct {
            tag: 0x72,
            fields: vec![
                PackValue::Int(9),
                PackValue::str("KNOWS"),
                PackValue::map::<&str>([]),
            ],
        };
        let p = PackValue::Struct {
            tag: 0x50,
            fields: vec![
                PackValue::List(vec![node(1, "P"), node(2, "P")]),
                PackValue::List(vec![rel]),
                PackValue::List(vec![PackValue::Int(-1), PackValue::Int(1)]),
            ],
        };
        let j = to_json(&p);
        let seq = j["$path"].as_array().expect("path");
        assert_eq!(seq.len(), 3);
        // A negative index: the relationship points back to the first node.
        assert_eq!(seq[1]["$rel"]["start"], json!(2));
        assert_eq!(seq[1]["$rel"]["end"], json!(1));
        assert_eq!(cell(&PackValue::Int(3)).as_deref(), Some("3"));
        assert!(cell(&node(1, "P")).is_some_and(|s| s.contains("$node")));
    }
}
