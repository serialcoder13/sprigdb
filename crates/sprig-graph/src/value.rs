//! Cypher values and their semantics (chapter 43, "Values"; build step
//! 10): equality and comparison with `null`, the global sort order, keys
//! for grouping and `DISTINCT`, and properties to and from the stored JSON
//! with chapter 31's type map.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use serde_json::{Map, Value as Json};
use sprig_common::{Result, SprigCode, SprigError};

/// Properties of a node or relationship.
pub type Props = BTreeMap<String, Value>;

/// A node.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// The graph.
    pub graph: u64,
    /// `id`.
    pub id: u64,
    /// Routing hash of `part`.
    pub rk: u64,
    /// Label names.
    pub labels: Vec<String>,
    /// Properties.
    pub props: Props,
}

/// A relationship.
#[derive(Debug, Clone, PartialEq)]
pub struct Rel {
    /// The graph.
    pub graph: u64,
    /// `id`.
    pub id: u64,
    /// Source node id.
    pub src: u64,
    /// Destination node id.
    pub dst: u64,
    /// Routing hash of the source (where the edge lives).
    pub src_rk: u64,
    /// Routing hash of the destination.
    pub dst_rk: u64,
    /// Type name.
    pub rtype: String,
    /// Properties.
    pub props: Props,
}

/// A path: `nodes.len() == rels.len() + 1`; `rels[i]` joins `nodes[i]` and
/// `nodes[i + 1]` in either direction.
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    /// The nodes in walk order.
    pub nodes: Vec<Arc<Node>>,
    /// The relationships in walk order.
    pub rels: Vec<Arc<Rel>>,
}

/// A Cypher value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `null`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A 64-bit integer.
    Int(i64),
    /// A float.
    Float(f64),
    /// A string.
    String(String),
    /// A list.
    List(Vec<Value>),
    /// A map.
    Map(BTreeMap<String, Value>),
    /// A node.
    Node(Arc<Node>),
    /// A relationship.
    Rel(Arc<Rel>),
    /// A path.
    Path(Arc<Path>),
}

/// Element id of a node: `n:<graph>:<rk hex>:<id>`.
#[must_use]
pub fn node_element_id(graph: u64, rk: u64, id: u64) -> String {
    format!("n:{graph}:{rk:x}:{id}")
}

/// Element id of a relationship: `r:<graph>:<source rk hex>:<id>`.
#[must_use]
pub fn rel_element_id(graph: u64, rk: u64, id: u64) -> String {
    format!("r:{graph}:{rk:x}:{id}")
}

/// Parses an element id: (kind `n` or `r`, graph, rk, id).
#[must_use]
pub fn parse_element_id(s: &str) -> Option<(char, u64, u64, u64)> {
    let mut it = s.split(':');
    let k = it.next()?;
    let kind = match k {
        "n" => 'n',
        "r" => 'r',
        _ => return None,
    };
    let g = it.next()?.parse().ok()?;
    let rk = u64::from_str_radix(it.next()?, 16).ok()?;
    let id = it.next()?.parse().ok()?;
    if it.next().is_some() {
        return None;
    }
    Some((kind, g, rk, id))
}

impl Node {
    /// `elementId(n)`.
    #[must_use]
    pub fn element_id(&self) -> String {
        node_element_id(self.graph, self.rk, self.id)
    }
}

impl Rel {
    /// `elementId(r)`.
    #[must_use]
    pub fn element_id(&self) -> String {
        rel_element_id(self.graph, self.src_rk, self.id)
    }
}

/// A grouping and `DISTINCT` key: values equal under Cypher's
/// equivalence compare equal (`1` and `1.0` too).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Key {
    /// `null`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A number: integers, and floats with an integral value.
    Int(i64),
    /// Other floats, by bits (NaN equals NaN here, as in grouping).
    Float(u64),
    /// A string.
    String(String),
    /// A list.
    List(Vec<Key>),
    /// A map.
    Map(Vec<(String, Key)>),
    /// A node by id.
    Node(u64),
    /// A relationship by id.
    Rel(u64),
    /// A path by its relationship and node ids.
    Path(Vec<u64>),
}

impl Value {
    /// A string value.
    #[must_use]
    pub fn str(s: impl Into<String>) -> Self {
        Value::String(s.into())
    }

    /// Whether this is `null`.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// The Cypher type name, for messages.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Bool(_) => "BOOLEAN",
            Value::Int(_) => "INTEGER",
            Value::Float(_) => "FLOAT",
            Value::String(_) => "STRING",
            Value::List(_) => "LIST",
            Value::Map(_) => "MAP",
            Value::Node(_) => "NODE",
            Value::Rel(_) => "RELATIONSHIP",
            Value::Path(_) => "PATH",
        }
    }

    /// The grouping key.
    #[must_use]
    pub fn key(&self) -> Key {
        match self {
            Value::Null => Key::Null,
            Value::Bool(b) => Key::Bool(*b),
            Value::Int(i) => Key::Int(*i),
            #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
            Value::Float(f) => {
                if f.fract() == 0.0 && f.abs() < 9.0e15 {
                    Key::Int(*f as i64)
                } else {
                    Key::Float(f.to_bits())
                }
            }
            Value::String(s) => Key::String(s.clone()),
            Value::List(l) => Key::List(l.iter().map(Value::key).collect()),
            Value::Map(m) => Key::Map(m.iter().map(|(k, v)| (k.clone(), v.key())).collect()),
            Value::Node(n) => Key::Node(n.id),
            Value::Rel(r) => Key::Rel(r.id),
            Value::Path(p) => Key::Path(
                p.nodes
                    .iter()
                    .map(|n| n.id)
                    .chain(p.rels.iter().map(|r| r.id))
                    .collect(),
            ),
        }
    }

    /// Cypher `=`: `None` is `null` (unknown).
    #[must_use]
    pub fn cypher_eq(&self, o: &Value) -> Option<bool> {
        #[allow(clippy::cast_precision_loss)]
        match (self, o) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Int(a), Value::Float(b)) | (Value::Float(b), Value::Int(a)) => {
                Some((*a as f64) == *b)
            }
            (Value::Int(a), Value::Int(b)) => Some(a == b),
            (Value::Float(a), Value::Float(b)) => Some(a == b),
            (Value::Bool(a), Value::Bool(b)) => Some(a == b),
            (Value::String(a), Value::String(b)) => Some(a == b),
            (Value::List(a), Value::List(b)) => {
                if a.len() != b.len() {
                    return Some(false);
                }
                let mut unknown = false;
                for (x, y) in a.iter().zip(b) {
                    match x.cypher_eq(y) {
                        Some(false) => return Some(false),
                        None => unknown = true,
                        Some(true) => {}
                    }
                }
                if unknown { None } else { Some(true) }
            }
            (Value::Map(a), Value::Map(b)) => {
                if a.len() != b.len() || a.keys().ne(b.keys()) {
                    return Some(false);
                }
                let mut unknown = false;
                for (x, y) in a.values().zip(b.values()) {
                    match x.cypher_eq(y) {
                        Some(false) => return Some(false),
                        None => unknown = true,
                        Some(true) => {}
                    }
                }
                if unknown { None } else { Some(true) }
            }
            (Value::Node(a), Value::Node(b)) => Some(a.id == b.id),
            (Value::Rel(a), Value::Rel(b)) => Some(a.id == b.id),
            (Value::Path(_), Value::Path(_)) => Some(self.key() == o.key()),
            _ => Some(false),
        }
    }

    /// Cypher `<` and friends: `None` for `null` or incomparable types.
    #[must_use]
    pub fn cypher_cmp(&self, o: &Value) -> Option<Ordering> {
        #[allow(clippy::cast_precision_loss)]
        match (self, o) {
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            (Value::List(a), Value::List(b)) => {
                for (x, y) in a.iter().zip(b) {
                    match x.cypher_cmp(y)? {
                        Ordering::Equal => {}
                        other => return Some(other),
                    }
                }
                Some(a.len().cmp(&b.len()))
            }
            _ => None,
        }
    }

    fn order_rank(&self) -> u8 {
        // Cypher's ascending global order: maps, nodes, relationships,
        // lists, paths, strings, booleans, numbers, then null last.
        match self {
            Value::Map(_) => 0,
            Value::Node(_) => 1,
            Value::Rel(_) => 2,
            Value::List(_) => 3,
            Value::Path(_) => 4,
            Value::String(_) => 5,
            Value::Bool(_) => 6,
            Value::Int(_) | Value::Float(_) => 7,
            Value::Null => 9,
        }
    }

    /// `ORDER BY`'s total order (ascending).
    #[must_use]
    pub fn order(&self, o: &Value) -> Ordering {
        let (a, b) = (self.order_rank(), o.order_rank());
        if a != b {
            return a.cmp(&b);
        }
        match (self, o) {
            (Value::Float(x), Value::Float(y)) => x.total_cmp(y),
            (Value::Int(_) | Value::Float(_), Value::Int(_) | Value::Float(_)) => {
                self.cypher_cmp(o).unwrap_or(Ordering::Equal)
            }
            (Value::List(x), Value::List(y)) => {
                for (p, q) in x.iter().zip(y) {
                    match p.order(q) {
                        Ordering::Equal => {}
                        other => return other,
                    }
                }
                x.len().cmp(&y.len())
            }
            (Value::Node(x), Value::Node(y)) => x.id.cmp(&y.id),
            (Value::Rel(x), Value::Rel(y)) => x.id.cmp(&y.id),
            _ => self.cypher_cmp(o).unwrap_or_else(|| self.key().cmp(&o.key())),
        }
    }

    /// The truth value for `WHERE`: `None` is `null`.
    ///
    /// # Errors
    /// 39909 for a non-boolean.
    pub fn truth(&self) -> Result<Option<bool>> {
        match self {
            Value::Null => Ok(None),
            Value::Bool(b) => Ok(Some(*b)),
            other => Err(type_error(format!(
                "expected a BOOLEAN, got {}",
                other.type_name()
            ))),
        }
    }

    /// The value as a string for `toString` and messages.
    #[must_use]
    pub fn display(&self) -> String {
        match self {
            Value::Null => "null".into(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => fmt_float(*f),
            Value::String(s) => s.clone(),
            Value::List(l) => {
                let parts: Vec<String> = l.iter().map(Value::literal).collect();
                format!("[{}]", parts.join(", "))
            }
            Value::Map(m) => {
                let parts: Vec<String> =
                    m.iter().map(|(k, v)| format!("{k}: {}", v.literal())).collect();
                format!("{{{}}}", parts.join(", "))
            }
            Value::Node(n) => format!("(:{} {})", n.labels.join(":"), props_literal(&n.props)),
            Value::Rel(r) => format!("[:{} {}]", r.rtype, props_literal(&r.props)),
            Value::Path(p) => format!("<path of {}>", p.rels.len()),
        }
    }

    fn literal(&self) -> String {
        match self {
            Value::String(s) => format!("\"{s}\""),
            other => other.display(),
        }
    }
}

fn props_literal(p: &Props) -> String {
    let mut s = String::from("{");
    for (i, (k, v)) in p.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let _ = write!(s, "{k}: {}", v.literal());
    }
    s.push('}');
    s
}

/// A float as Cypher prints it: always with a fraction or exponent.
#[must_use]
pub fn fmt_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    let s = format!("{f:?}");
    if s.contains('.') || s.contains('e') || s.contains("inf") {
        s
    } else {
        format!("{s}.0")
    }
}

/// A 39909 type error.
#[must_use]
pub fn type_error(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::GraphPropertyType, msg)
}

// ----------------------------------------------------------------------
// Properties as stored: JSON plus chapter 31's type map.
// ----------------------------------------------------------------------

/// Checks that `v` may be stored as a property: Neo4j's property types
/// (no maps, no graph elements, homogeneous lists of primitives).
///
/// # Errors
/// 39909 otherwise.
pub fn check_property(key: &str, v: &Value) -> Result<()> {
    match v {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::String(_) => Ok(()),
        Value::List(l) => {
            let mut kind: Option<&'static str> = None;
            for x in l {
                if matches!(x, Value::List(_) | Value::Map(_) | Value::Null)
                    || matches!(x, Value::Node(_) | Value::Rel(_) | Value::Path(_))
                {
                    return Err(type_error(format!(
                        "property '{key}': a list property holds only non-null simple values"
                    )));
                }
                let t = match x {
                    Value::Int(_) | Value::Float(_) => "number",
                    other => other.type_name(),
                };
                if kind.is_some_and(|k| k != t) {
                    return Err(type_error(format!(
                        "property '{key}': list items must all have one type"
                    )));
                }
                kind = Some(t);
            }
            Ok(())
        }
        other => Err(type_error(format!(
            "property '{key}': a {} can't be stored as a property value",
            other.type_name()
        ))),
    }
}

fn to_json_inner(v: &Value, path: &str, types: &mut Map<String, Json>) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::Int(i) => Json::from(*i),
        Value::Float(f) => {
            if f.fract() == 0.0 || !f.is_finite() {
                types.insert(path.to_owned(), Json::from("float"));
            }
            serde_json::Number::from_f64(*f).map_or(Json::Null, Json::Number)
        }
        Value::String(s) => Json::String(s.clone()),
        Value::List(l) => Json::Array(
            l.iter()
                .enumerate()
                .map(|(i, x)| to_json_inner(x, &format!("{path}[{i}]"), types))
                .collect(),
        ),
        Value::Map(m) => Json::Object(
            m.iter()
                .map(|(k, x)| (k.clone(), to_json_inner(x, &format!("{path}.{k}"), types)))
                .collect(),
        ),
        Value::Node(_) | Value::Rel(_) | Value::Path(_) => Json::String(v.display()),
    }
}

/// Properties as stored: the `props` object and the `_types` map (`None`
/// when empty). Floats with an integral value get a `float` entry.
#[must_use]
pub fn props_to_json(p: &Props) -> (Json, Option<Json>) {
    let mut types = Map::new();
    let mut obj = Map::new();
    for (k, v) in p {
        if v.is_null() {
            continue;
        }
        obj.insert(k.clone(), to_json_inner(v, k, &mut types));
    }
    let t = if types.is_empty() {
        None
    } else {
        Some(Json::Object(types))
    };
    (Json::Object(obj), t)
}

fn from_json_inner(j: &Json, path: &str, types: Option<&Map<String, Json>>) -> Value {
    match j {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => {
            let is_float = types
                .and_then(|t| t.get(path))
                .and_then(Json::as_str)
                .is_some_and(|t| t == "float");
            match (n.as_i64(), is_float) {
                #[allow(clippy::cast_precision_loss)]
                (Some(i), true) => Value::Float(i as f64),
                (Some(i), false) => Value::Int(i),
                (None, _) => Value::Float(n.as_f64().unwrap_or(0.0)),
            }
        }
        Json::String(s) => Value::String(s.clone()),
        Json::Array(a) => Value::List(
            a.iter()
                .enumerate()
                .map(|(i, x)| from_json_inner(x, &format!("{path}[{i}]"), types))
                .collect(),
        ),
        Json::Object(o) => Value::Map(
            o.iter()
                .map(|(k, x)| (k.clone(), from_json_inner(x, &format!("{path}.{k}"), types)))
                .collect(),
        ),
    }
}

/// Properties from the stored `props` and `_types`.
#[must_use]
pub fn props_from_json(props: &Json, types: Option<&Json>) -> Props {
    let t = types.and_then(Json::as_object);
    match props {
        Json::Object(o) => o
            .iter()
            .map(|(k, v)| (k.clone(), from_json_inner(v, k, t)))
            .collect(),
        _ => Props::new(),
    }
}

/// A parameter or literal from JSON (no type map: integers stay integers).
#[must_use]
pub fn from_json(j: &Json) -> Value {
    from_json_inner(j, "", None)
}

/// A value as plain JSON (graph elements as maps), for SQL results.
#[must_use]
pub fn to_plain_json(v: &Value) -> Json {
    match v {
        Value::Node(n) => serde_json::json!({
            "elementId": n.element_id(),
            "labels": n.labels,
            "properties": props_to_json(&n.props).0,
        }),
        Value::Rel(r) => serde_json::json!({
            "elementId": r.element_id(),
            "type": r.rtype,
            "properties": props_to_json(&r.props).0,
        }),
        Value::Path(p) => serde_json::json!({
            "nodes": p.nodes.iter().map(|n| to_plain_json(&Value::Node(n.clone()))).collect::<Vec<_>>(),
            "relationships": p.rels.iter().map(|r| to_plain_json(&Value::Rel(r.clone()))).collect::<Vec<_>>(),
        }),
        Value::List(l) => Json::Array(l.iter().map(to_plain_json).collect()),
        Value::Map(m) => Json::Object(m.iter().map(|(k, x)| (k.clone(), to_plain_json(x))).collect()),
        other => to_json_inner(other, "", &mut Map::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality_follows_cypher() {
        assert_eq!(Value::Int(1).cypher_eq(&Value::Float(1.0)), Some(true));
        assert_eq!(Value::Int(1).cypher_eq(&Value::str("1")), Some(false));
        assert_eq!(Value::Int(1).cypher_eq(&Value::Null), None);
        assert_eq!(
            Value::List(vec![Value::Int(1), Value::Null]).cypher_eq(&Value::List(vec![
                Value::Int(2),
                Value::Null
            ])),
            Some(false)
        );
        assert_eq!(
            Value::List(vec![Value::Int(1), Value::Null])
                .cypher_eq(&Value::List(vec![Value::Int(1), Value::Null])),
            None
        );
        assert_eq!(Value::Int(1).cypher_cmp(&Value::str("a")), None);
        assert_eq!(Value::Int(1).key(), Value::Float(1.0).key());
    }

    #[test]
    fn global_order_puts_null_last() {
        let mut v = [
            Value::Null,
            Value::Int(2),
            Value::str("b"),
            Value::Float(1.5),
            Value::Bool(true),
            Value::str("a"),
        ];
        v.sort_by(Value::order);
        assert_eq!(
            v,
            [
                Value::str("a"),
                Value::str("b"),
                Value::Bool(true),
                Value::Float(1.5),
                Value::Int(2),
                Value::Null
            ]
        );
    }

    #[test]
    fn properties_round_trip_with_the_type_map() {
        let mut p = Props::new();
        p.insert("a".into(), Value::Int(1));
        p.insert("b".into(), Value::Float(1.0));
        p.insert("c".into(), Value::Float(2.5));
        p.insert("d".into(), Value::List(vec![Value::Float(3.0), Value::Float(1.5)]));
        p.insert("e".into(), Value::str("x"));
        let (j, t) = props_to_json(&p);
        let t = t.expect("types");
        assert_eq!(t["b"], "float");
        assert_eq!(t["d[0]"], "float");
        // Even if MariaDB rewrote 1.0 as 1, the type map restores it.
        let rewritten: Json = serde_json::from_str(
            &j.to_string().replace("1.0", "1").replace("3.0", "3"),
        )
        .expect("json");
        assert_eq!(props_from_json(&rewritten, Some(&t)), p);
        assert!(check_property("m", &Value::Map(BTreeMap::new())).is_err());
        assert!(check_property("l", &Value::List(vec![Value::Int(1), Value::str("x")])).is_err());
        assert!(check_property("l", &Value::List(vec![Value::Int(1), Value::Float(2.0)])).is_ok());
    }

    #[test]
    fn element_ids_round_trip() {
        let e = node_element_id(901, 0xabc, 1001);
        assert_eq!(e, "n:901:abc:1001");
        assert_eq!(parse_element_id(&e), Some(('n', 901, 0xabc, 1001)));
        assert_eq!(parse_element_id("x:1:2:3"), None);
        assert_eq!(fmt_float(1.0), "1.0");
        assert_eq!(fmt_float(0.1), "0.1");
    }
}
