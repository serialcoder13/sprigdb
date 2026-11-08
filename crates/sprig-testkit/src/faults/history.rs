//! Operation histories (chapter 46, "Data structures and storage":
//! "JSON lines of `OpRecord`, converted to EDN for `elle-cli`").
//!
//! A workload records an `invoke` before each operation and an `ok`,
//! `fail` or `info` (outcome unknown, such as a timeout) after it. For
//! Elle's list-append and rw-register models the operation is `f = "txn"`
//! and the value a list of micro-operations, `["append", key, value]`,
//! `["r", key, null-or-list]` or `["w", key, value]`; the EDN conversion
//! turns their names into keywords.

use std::fmt::Write as _;
use std::io::{BufRead, Write as _};
use std::path::Path;
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_common::{Result, SprigError};

/// The type of a history entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpKind {
    /// The operation started.
    Invoke,
    /// It took effect.
    Ok,
    /// It certainly did not take effect.
    Fail,
    /// Unknown whether it took effect.
    Info,
}

impl OpKind {
    fn edn(self) -> &'static str {
        match self {
            OpKind::Invoke => ":invoke",
            OpKind::Ok => ":ok",
            OpKind::Fail => ":fail",
            OpKind::Info => ":info",
        }
    }
}

/// One history entry (chapter 46, "Interfaces").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpRecord {
    /// The logical client.
    pub process: u32,
    /// Invoke, ok, fail or info.
    #[serde(rename = "type")]
    pub kind: OpKind,
    /// The operation, such as `txn`.
    pub f: String,
    /// Its argument or result.
    pub value: Value,
    /// Nanoseconds since the history started.
    pub time_ns: u64,
}

/// What a checker concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// `Some(true)` valid, `Some(false)` invalid, `None` unknown.
    pub valid: Option<bool>,
    /// Anomalies found, such as `G1c`.
    pub anomalies: Vec<String>,
    /// The checker's full output, for reports.
    pub detail: String,
}

/// A history checker (chapter 46, "Interfaces").
pub trait Checker {
    /// Checks a whole history.
    fn check(&self, history: &[OpRecord]) -> Verdict;
}

/// A history being recorded by concurrent workers.
#[derive(Debug)]
pub struct History {
    start: Instant,
    records: Mutex<Vec<OpRecord>>,
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}

impl History {
    /// An empty history starting now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            records: Mutex::new(Vec::new()),
        }
    }

    /// Appends an entry stamped with the time since the start.
    pub fn record(&self, process: u32, kind: OpKind, f: &str, value: Value) {
        let time_ns = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(OpRecord {
                process,
                kind,
                f: f.to_owned(),
                value,
                time_ns,
            });
    }

    /// Every entry, in the order recorded.
    #[must_use]
    pub fn records(&self) -> Vec<OpRecord> {
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Writes `records` as JSON lines.
///
/// # Errors
/// If the file can't be written.
pub fn write_jsonl(path: &Path, records: &[OpRecord]) -> Result<()> {
    let io = |e: std::io::Error| {
        SprigError::internal(format!("writing {}", path.display())).with_source(e)
    };
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).map_err(io)?);
    for r in records {
        let line = serde_json::to_string(r)
            .map_err(|e| SprigError::internal(format!("encoding history: {e}")))?;
        writeln!(f, "{line}").map_err(io)?;
    }
    f.flush().map_err(io)
}

/// Reads JSON lines written by [`write_jsonl`].
///
/// # Errors
/// If the file can't be read or a line doesn't parse.
pub fn read_jsonl(path: &Path) -> Result<Vec<OpRecord>> {
    let f = std::fs::File::open(path)
        .map_err(|e| SprigError::internal(format!("reading {}", path.display())).with_source(e))?;
    std::io::BufReader::new(f)
        .lines()
        .filter(|l| l.as_ref().map_or(true, |l| !l.trim().is_empty()))
        .map(|l| {
            let l = l.map_err(|e| SprigError::internal("reading history").with_source(e))?;
            serde_json::from_str(&l)
                .map_err(|e| SprigError::internal(format!("history line {l:?}: {e}")))
        })
        .collect()
}

/// The history in Elle's EDN form, one operation map per line, with
/// `:index` added.
#[must_use]
pub fn to_edn(records: &[OpRecord]) -> String {
    let mut s = String::new();
    for (i, r) in records.iter().enumerate() {
        let _ = write!(
            s,
            "{{:index {i}, :type {}, :process {}, :time {}, :f {}",
            r.kind.edn(),
            r.process,
            r.time_ns,
            keyword(&r.f)
        );
        if !r.value.is_null() {
            s.push_str(", :value ");
            edn_value(&r.value, r.f == "txn", &mut s);
        }
        s.push_str("}\n");
    }
    s
}

fn keyword(name: &str) -> String {
    let clean: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || "-_?!*+".contains(c) {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!(":{clean}")
}

/// JSON to EDN. In a `txn` value, each micro-operation's first element
/// becomes a keyword.
fn edn_value(v: &Value, txn: bool, out: &mut String) {
    match v {
        Value::Array(items) if txn => {
            out.push('[');
            for (i, mop) in items.iter().enumerate() {
                if i > 0 {
                    out.push(' ');
                }
                match mop {
                    Value::Array(parts) if !parts.is_empty() => {
                        out.push('[');
                        for (j, p) in parts.iter().enumerate() {
                            if j > 0 {
                                out.push(' ');
                            }
                            match (j, p) {
                                (0, Value::String(f)) => out.push_str(&keyword(f)),
                                _ => edn_value(p, false, out),
                            }
                        }
                        out.push(']');
                    }
                    other => edn_value(other, false, out),
                }
            }
            out.push(']');
        }
        Value::Null => out.push_str("nil"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => {
            out.push('"');
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, x) in items.iter().enumerate() {
                if i > 0 {
                    out.push(' ');
                }
                edn_value(x, false, out);
            }
            out.push(']');
        }
        Value::Object(m) => {
            out.push('{');
            for (i, (k, x)) in m.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                // Numeric keys stay numbers (bank histories use them).
                if k.parse::<i64>().is_ok() {
                    out.push_str(k);
                } else {
                    out.push_str(&keyword(k));
                }
                out.push(' ');
                edn_value(x, false, out);
            }
            out.push('}');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn edn_matches_elles_format() {
        let h = History::new();
        h.record(
            0,
            OpKind::Invoke,
            "txn",
            json!([["append", 1, 2], ["r", 3, null]]),
        );
        h.record(
            0,
            OpKind::Ok,
            "txn",
            json!([["append", 1, 2], ["r", 3, [1, 2]]]),
        );
        h.record(1, OpKind::Invoke, "read", Value::Null);
        h.record(1, OpKind::Ok, "read", json!({"0": 10, "1": 0}));
        h.record(2, OpKind::Info, "write", json!({"from": "a\"b"}));
        let edn = to_edn(&h.records());
        let lines: Vec<&str> = edn.lines().collect();
        assert!(lines[0].starts_with("{:index 0, :type :invoke, :process 0, :time "));
        assert!(
            lines[0].ends_with(":f :txn, :value [[:append 1 2] [:r 3 nil]]}"),
            "{}",
            lines[0]
        );
        assert!(
            lines[1].ends_with(":value [[:append 1 2] [:r 3 [1 2]]]}"),
            "{}",
            lines[1]
        );
        assert!(lines[2].ends_with(":f :read}"), "{}", lines[2]);
        assert!(lines[3].ends_with(":value {0 10, 1 0}}"), "{}", lines[3]);
        assert!(
            lines[4].ends_with(":value {:from \"a\\\"b\"}}"),
            "{}",
            lines[4]
        );
    }

    #[test]
    fn jsonl_round_trips() {
        let dir = crate::util::TempDir::new("history").unwrap();
        let h = History::new();
        h.record(3, OpKind::Fail, "txn", json!([["w", 1, 5]]));
        let p = dir.join("h.jsonl");
        write_jsonl(&p, &h.records()).unwrap();
        assert_eq!(read_jsonl(&p).unwrap(), h.records());
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(text.contains("\"type\":\"fail\""), "{text}");
    }
}
