//! The openCypher TCK (chapter 43, "Testing"; build step 29) through a
//! small Gherkin runner of our own, on the in-memory store with 1 and 4
//! shards (the MariaDB store answers as memory does: the differential and
//! store tests show it).
//!
//! The feature files (Apache-2.0) are test data, not part of the
//! repository: `SPRIG_TCK_DIR` names the `tck/features` folder of an
//! openCypher checkout (round 3 used tag `2024.3`, commit
//! `677cbafabb8c3c5eed458fd3b1ec0daec8d67d23`). Without it the test skips.
//!
//! A scenario passes when every step does: setup queries run, the query's
//! rows equal the expected table (values compared in a normalized text
//! form: labels and map keys sorted, floats by value), the expected error
//! is raised (any error counts; its classification isn't compared), and
//! the side effects match Neo4j's counters (`-properties` isn't counted by
//! the executor and is skipped). Scenarios needing procedures, the binary
//! or features the runner doesn't model are counted as skipped, with the
//! reason. `SPRIG_TCK_REPORT` writes every failure to a file.

#![allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::enum_variant_names,
    clippy::match_same_arms,
    clippy::assigning_clones,
    clippy::format_collect
)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use sprig_graph::eval::Params;
use sprig_graph::mem::MemStore;
use sprig_graph::store::GraphStore;
use sprig_graph::value::Value;
use sprig_graph::{Budgets, Output, execute};

#[derive(Debug, Clone)]
enum Step {
    Given,
    HavingExecuted(String),
    Params(Vec<Vec<String>>),
    Executing(String),
    Result { mode: Mode, table: Vec<Vec<String>> },
    Empty,
    Error(String),
    NoSideEffects,
    SideEffects(Vec<Vec<String>>),
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    AnyOrder,
    InOrder,
    IgnoringListOrder,
}

#[derive(Debug, Clone)]
struct Scenario {
    feature: String,
    name: String,
    steps: Vec<Step>,
}

fn cells(line: &str) -> Vec<String> {
    let t = line.trim();
    let t = t.strip_prefix('|').unwrap_or(t);
    let t = t.strip_suffix('|').unwrap_or(t);
    // Cells split at `|` outside quotes.
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut esc = false;
    for c in t.chars() {
        if esc {
            cur.push(c);
            esc = false;
            continue;
        }
        if c == '\\' {
            cur.push(c);
            esc = true;
            continue;
        }
        match quote {
            Some(q) if c == q => {
                quote = None;
                cur.push(c);
            }
            Some(_) => cur.push(c),
            None if c == '\'' || c == '"' => {
                quote = Some(c);
                cur.push(c);
            }
            None if c == '|' => out.push(std::mem::take(&mut cur).trim().to_owned()),
            None => cur.push(c),
        }
    }
    out.push(cur.trim().to_owned());
    out
}

fn parse_feature(path: &Path) -> Vec<Scenario> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    let mut feature = String::new();
    let mut out = Vec::new();
    let mut i = 0;
    // (scenario, is outline)
    let mut cur: Option<(Scenario, bool)> = None;
    let mut examples: Vec<Vec<String>> = Vec::new();
    let flush = |cur: &mut Option<(Scenario, bool)>,
                 examples: &mut Vec<Vec<String>>,
                 out: &mut Vec<Scenario>| {
        if let Some((s, outline)) = cur.take() {
            if outline {
                if examples.len() > 1 {
                    let head = examples[0].clone();
                    for (k, row) in examples.iter().skip(1).enumerate() {
                        let sub = |x: &str| {
                            let mut y = x.to_owned();
                            for (h, v) in head.iter().zip(row) {
                                y = y.replace(&format!("<{h}>"), v);
                            }
                            y
                        };
                        let steps = s
                            .steps
                            .iter()
                            .map(|st| match st {
                                Step::HavingExecuted(q) => Step::HavingExecuted(sub(q)),
                                Step::Executing(q) => Step::Executing(sub(q)),
                                Step::Result { mode, table } => Step::Result {
                                    mode: *mode,
                                    table: table
                                        .iter()
                                        .map(|r| r.iter().map(|c| sub(c)).collect())
                                        .collect(),
                                },
                                Step::Params(t) => Step::Params(
                                    t.iter()
                                        .map(|r| r.iter().map(|c| sub(c)).collect())
                                        .collect(),
                                ),
                                Step::Error(e) => Step::Error(sub(e)),
                                Step::SideEffects(t) => Step::SideEffects(
                                    t.iter()
                                        .map(|r| r.iter().map(|c| sub(c)).collect())
                                        .collect(),
                                ),
                                other => other.clone(),
                            })
                            .collect();
                        out.push(Scenario {
                            feature: s.feature.clone(),
                            name: format!("{} (example {})", s.name, k + 1),
                            steps,
                        });
                    }
                }
            } else {
                out.push(s);
            }
        }
        examples.clear();
    };
    let mut in_examples = false;
    let mut in_background = false;
    let mut background: Vec<Step> = Vec::new();
    while i < lines.len() {
        let l = lines[i].trim();
        if let Some(f) = l.strip_prefix("Feature:") {
            feature = f.trim().to_owned();
        } else if l.starts_with("Background:") {
            flush(&mut cur, &mut examples, &mut out);
            in_background = true;
            cur = Some((
                Scenario {
                    feature: feature.clone(),
                    name: "background".into(),
                    steps: Vec::new(),
                },
                false,
            ));
        } else if l.starts_with("Scenario Outline:") || l.starts_with("Scenario:") {
            if in_background {
                background = cur.take().map(|(s, _)| s.steps).unwrap_or_default();
                in_background = false;
            }
            flush(&mut cur, &mut examples, &mut out);
            in_examples = false;
            let outline = l.starts_with("Scenario Outline:");
            let name = l.split_once(':').map_or("", |x| x.1).trim().to_owned();
            cur = Some((
                Scenario {
                    feature: feature.clone(),
                    name,
                    steps: background.clone(),
                },
                outline,
            ));
        } else if l.starts_with("Examples:") {
            in_examples = true;
        } else if l.starts_with('|') && in_examples {
            examples.push(cells(l));
        } else if let Some(rest) = ["Given ", "When ", "Then ", "And ", "But "]
            .iter()
            .find_map(|k| l.strip_prefix(k))
        {
            let rest = rest.trim_end_matches(':').trim();
            // A docstring or a table may follow.
            let mut doc = None;
            let mut table: Vec<Vec<String>> = Vec::new();
            let mut j = i + 1;
            if lines.get(j).is_some_and(|x| x.trim() == "\"\"\"") {
                let mut d = Vec::new();
                j += 1;
                while j < lines.len() && lines[j].trim() != "\"\"\"" {
                    d.push(lines[j].trim_start());
                    j += 1;
                }
                doc = Some(d.join("\n"));
                j += 1;
            } else {
                while j < lines.len() && lines[j].trim().starts_with('|') {
                    table.push(cells(lines[j]));
                    j += 1;
                }
            }
            i = j - 1;
            let step = if rest.starts_with("having executed") {
                Step::HavingExecuted(doc.unwrap_or_default())
            } else if rest.starts_with("executing query")
                || rest.starts_with("executing control query")
            {
                Step::Executing(doc.unwrap_or_default())
            } else if rest.starts_with("parameters are") {
                Step::Params(table)
            } else if rest == "the result should be empty" {
                Step::Empty
            } else if let Some(m) = rest.strip_prefix("the result should be") {
                let mode = if m.contains("in order") {
                    Mode::InOrder
                } else if m.contains("ignoring element order") {
                    Mode::IgnoringListOrder
                } else {
                    Mode::AnyOrder
                };
                Step::Result { mode, table }
            } else if rest.starts_with("a ") && rest.contains("should be raised") {
                Step::Error(rest.to_owned())
            } else if rest == "no side effects" {
                Step::NoSideEffects
            } else if rest.starts_with("the side effects should be") {
                Step::SideEffects(table)
            } else if rest == "an empty graph" || rest == "any graph" {
                Step::Given
            } else {
                Step::Other(rest.to_owned())
            };
            if let Some((s, _)) = cur.as_mut() {
                s.steps.push(step);
            }
        }
        i += 1;
    }
    flush(&mut cur, &mut examples, &mut out);
    out
}

// ---------------------------------------------------------------------
// Values in a normalized text form.

fn fmt_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    format!("f{f:e}")
}

fn fmt_map(m: &BTreeMap<String, String>) -> String {
    let parts: Vec<String> = m.iter().map(|(k, v)| format!("{k}:{v}")).collect();
    format!("{{{}}}", parts.join(","))
}

fn fmt_props(p: &BTreeMap<String, Value>, list_unordered: bool) -> String {
    let m: BTreeMap<String, String> = p
        .iter()
        .filter(|(_, v)| !matches!(v, Value::Null))
        .map(|(k, v)| (k.clone(), fmt(v, list_unordered)))
        .collect();
    if m.is_empty() {
        String::new()
    } else {
        fmt_map(&m)
    }
}

fn fmt(v: &Value, list_unordered: bool) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => format!("i{i}"),
        Value::Float(f) => fmt_float(*f),
        Value::String(s) => format!("'{s}'"),
        Value::List(l) => {
            let mut items: Vec<String> = l.iter().map(|x| fmt(x, list_unordered)).collect();
            if list_unordered {
                items.sort();
            }
            format!("[{}]", items.join(","))
        }
        Value::Map(m) => fmt_map(
            &m.iter()
                .map(|(k, v)| (k.clone(), fmt(v, list_unordered)))
                .collect(),
        ),
        Value::Node(n) => {
            let mut labels = n.labels.clone();
            labels.sort();
            let props: BTreeMap<String, Value> = n
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            format!(
                "({}{})",
                labels.iter().map(|l| format!(":{l}")).collect::<String>(),
                fmt_props(&props, list_unordered)
            )
        }
        Value::Rel(r) => {
            let props: BTreeMap<String, Value> = r
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            format!("[:{}{}]", r.rtype, fmt_props(&props, list_unordered))
        }
        Value::Path(p) => {
            let mut s = String::from("<");
            s.push_str(&fmt(&Value::Node(p.nodes[0].clone()), list_unordered));
            for (i, r) in p.rels.iter().enumerate() {
                let rv = fmt(&Value::Rel(r.clone()), list_unordered);
                if r.src == p.nodes[i].id {
                    let _ = write!(s, "-{rv}->");
                } else {
                    let _ = write!(s, "<-{rv}-");
                }
                s.push_str(&fmt(&Value::Node(p.nodes[i + 1].clone()), list_unordered));
            }
            s.push('>');
            s
        }
        other => format!(
            "'{}'",
            java_text(&sprig_graph::temporal::text(other).unwrap_or_else(|| other.display()))
        ),
    }
}

/// The TCK's expected temporal texts are Java's `toString` forms, which
/// leave out zero seconds (`10:35`, not `10:35:00`); Cypher's `toString()`
/// keeps them.
fn java_text(t: &str) -> String {
    let b = t.as_bytes();
    let mut out = String::with_capacity(t.len());
    let mut i = 0;
    while i < b.len() {
        // `HH:MM:00` not followed by a fraction.
        if i + 8 <= b.len()
            && b[i].is_ascii_digit()
            && b[i + 1].is_ascii_digit()
            && b[i + 2] == b':'
            && b[i + 3].is_ascii_digit()
            && b[i + 4].is_ascii_digit()
            && &b[i + 5..i + 8] == b":00"
            && b.get(i + 8) != Some(&b'.')
            && (i == 0 || !b[i - 1].is_ascii_digit())
        {
            out.push_str(&t[i..i + 5]);
            i += 8;
            continue;
        }
        out.push(b[i] as char);
        i += 1;
    }
    out
}

/// A parser of the TCK's expected values into the same text form.
struct Exp<'a> {
    s: &'a [u8],
    i: usize,
    unordered: bool,
}

impl Exp<'_> {
    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn eat(&mut self, c: u8) -> bool {
        self.ws();
        if self.s.get(self.i) == Some(&c) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn ident(&mut self) -> Option<String> {
        self.ws();
        if self.s.get(self.i) == Some(&b'`') {
            let st = self.i + 1;
            let mut j = st;
            while j < self.s.len() && self.s[j] != b'`' {
                j += 1;
            }
            self.i = j + 1;
            return Some(String::from_utf8_lossy(&self.s[st..j]).into_owned());
        }
        let st = self.i;
        while self.i < self.s.len()
            && (self.s[self.i].is_ascii_alphanumeric() || self.s[self.i] == b'_')
        {
            self.i += 1;
        }
        (self.i > st).then(|| String::from_utf8_lossy(&self.s[st..self.i]).into_owned())
    }
    fn props(&mut self) -> Option<String> {
        self.ws();
        if self.s.get(self.i) != Some(&b'{') {
            return Some(String::new());
        }
        let m = self.map()?;
        Some(m)
    }
    fn map(&mut self) -> Option<String> {
        if !self.eat(b'{') {
            return None;
        }
        let mut m = BTreeMap::new();
        if !self.eat(b'}') {
            loop {
                let k = self.ident()?;
                if !self.eat(b':') {
                    return None;
                }
                let v = self.value()?;
                if v != "null" {
                    m.insert(k, v);
                }
                if self.eat(b'}') {
                    break;
                }
                if !self.eat(b',') {
                    return None;
                }
            }
        }
        Some(if m.is_empty() {
            String::new()
        } else {
            fmt_map(&m)
        })
    }
    fn node(&mut self) -> Option<String> {
        if !self.eat(b'(') {
            return None;
        }
        let mut labels = Vec::new();
        while self.eat(b':') {
            labels.push(self.ident()?);
        }
        labels.sort();
        let p = self.props()?;
        if !self.eat(b')') {
            return None;
        }
        Some(format!(
            "({}{p})",
            labels.iter().map(|l| format!(":{l}")).collect::<String>()
        ))
    }
    fn rel(&mut self) -> Option<String> {
        if !self.eat(b'[') {
            return None;
        }
        if !self.eat(b':') {
            return None;
        }
        let t = self.ident()?;
        let p = self.props()?;
        if !self.eat(b']') {
            return None;
        }
        Some(format!("[:{t}{p}]"))
    }
    fn value(&mut self) -> Option<String> {
        self.ws();
        let c = *self.s.get(self.i)?;
        match c {
            b'\'' | b'"' => {
                let q = c;
                self.i += 1;
                let mut out = Vec::new();
                while self.i < self.s.len() && self.s[self.i] != q {
                    if self.s[self.i] == b'\\' && self.i + 1 < self.s.len() {
                        self.i += 1;
                    }
                    out.push(self.s[self.i]);
                    self.i += 1;
                }
                self.i += 1;
                Some(format!("'{}'", String::from_utf8_lossy(&out)))
            }
            b'[' => {
                // A list, or a relationship.
                let save = self.i;
                if let Some(r) = self.rel() {
                    return Some(r);
                }
                self.i = save + 1;
                let mut items = Vec::new();
                if !self.eat(b']') {
                    loop {
                        items.push(self.value()?);
                        if self.eat(b']') {
                            break;
                        }
                        if !self.eat(b',') {
                            return None;
                        }
                    }
                }
                if self.unordered {
                    items.sort();
                }
                Some(format!("[{}]", items.join(",")))
            }
            b'{' => self
                .map()
                .map(|m| if m.is_empty() { "{}".into() } else { m }),
            b'(' => self.node(),
            b'<' => {
                self.i += 1;
                let mut s = String::from("<");
                s.push_str(&self.node()?);
                loop {
                    self.ws();
                    if self.eat(b'>') {
                        break;
                    }
                    if self.eat(b'<') {
                        if !self.eat(b'-') {
                            return None;
                        }
                        let r = self.rel()?;
                        if !self.eat(b'-') {
                            return None;
                        }
                        let _ = write!(s, "<-{r}-");
                    } else if self.eat(b'-') {
                        let r = self.rel()?;
                        if !(self.eat(b'-') && self.eat(b'>')) {
                            return None;
                        }
                        let _ = write!(s, "-{r}->");
                    } else {
                        return None;
                    }
                    s.push_str(&self.node()?);
                }
                s.push('>');
                Some(s)
            }
            _ => {
                let st = self.i;
                while self.i < self.s.len()
                    && !matches!(self.s[self.i], b',' | b']' | b'}' | b')')
                    && !self.s[self.i].is_ascii_whitespace()
                {
                    self.i += 1;
                }
                let w = String::from_utf8_lossy(&self.s[st..self.i]).into_owned();
                match w.as_str() {
                    "null" => Some("null".into()),
                    "true" | "false" => Some(w),
                    "NaN" => Some("NaN".into()),
                    "Infinity" | "+Infinity" => Some("Infinity".into()),
                    "-Infinity" => Some("-Infinity".into()),
                    _ => {
                        if let Ok(i) = w.parse::<i64>() {
                            Some(format!("i{i}"))
                        } else if let Some(h) = w.strip_prefix("0x") {
                            i64::from_str_radix(h, 16).ok().map(|i| format!("i{i}"))
                        } else if let Some(h) = w.strip_prefix("-0x") {
                            i64::from_str_radix(h, 16).ok().map(|i| format!("i{}", -i))
                        } else {
                            w.parse::<f64>().ok().map(fmt_float)
                        }
                    }
                }
            }
        }
    }
}

fn expected(cell: &str, unordered: bool) -> Option<String> {
    let mut p = Exp {
        s: cell.as_bytes(),
        i: 0,
        unordered,
    };
    let v = p.value()?;
    p.ws();
    (p.i == p.s.len()).then_some(v)
}

/// A parameter value from its TCK text (through the executor: `RETURN e`).
async fn param_value(text: &str) -> Option<Value> {
    let mut m = MemStore::new(1);
    let o = execute(
        &mut m,
        &format!("RETURN {text} AS v"),
        &Params::new(),
        &Budgets::default(),
    )
    .await
    .ok()?;
    o.rows.first().and_then(|r| r.first()).cloned()
}

#[derive(Debug)]
enum Outcome {
    Pass,
    Fail(String),
    Skip(String),
}

fn counts(stats: &BTreeMap<String, i64>) -> BTreeMap<&'static str, i64> {
    let mut m = BTreeMap::new();
    for (tck, ours) in [
        ("+nodes", "nodes-created"),
        ("-nodes", "nodes-deleted"),
        ("+relationships", "relationships-created"),
        ("-relationships", "relationships-deleted"),
        ("+labels", "labels-added"),
        ("-labels", "labels-removed"),
        ("+properties", "properties-set"),
    ] {
        let v = stats.get(ours).copied().unwrap_or(0);
        if v != 0 {
            m.insert(tck, v);
        }
    }
    m
}

async fn run(s: &Scenario, shards: usize) -> Outcome {
    let mut store = MemStore::new(shards);
    let mut params = Params::new();
    let mut result: Option<Result<Output, sprig_common::SprigError>> = None;
    let mut stats: BTreeMap<String, i64> = BTreeMap::new();
    for st in &s.steps {
        match st {
            Step::Given => {}
            Step::Other(o) => return Outcome::Skip(o.clone()),
            Step::HavingExecuted(q) => {
                if let Err(e) = execute(&mut store, q, &Params::new(), &Budgets::default()).await {
                    return Outcome::Fail(format!("setup failed: {e}"));
                }
            }
            Step::Params(t) => {
                for row in t {
                    if row.len() < 2 {
                        continue;
                    }
                    let Some(v) = param_value(&row[1]).await else {
                        return Outcome::Skip(format!("parameter {}", row[1]));
                    };
                    params.insert(row[0].clone(), v);
                }
            }
            Step::Executing(q) => {
                let before: std::collections::BTreeSet<String> =
                    store.all_labels().into_iter().collect();
                let r = execute(&mut store, q, &params, &Budgets::default()).await;
                if let Ok(o) = &r {
                    stats.clone_from(&o.stats);
                    // The TCK's `+labels` counts label names new to the
                    // graph; Neo4j's `labels-added` counts per node.
                    let new = store
                        .all_labels()
                        .into_iter()
                        .filter(|l| !before.contains(l))
                        .count();
                    stats.insert("labels-added".into(), i64::try_from(new).unwrap_or(0));
                }
                result = Some(r);
            }
            Step::Empty => match &result {
                Some(Ok(o)) if o.rows.is_empty() => {}
                Some(Ok(o)) => {
                    return Outcome::Fail(format!("expected no rows, got {}", o.rows.len()));
                }
                Some(Err(e)) => return Outcome::Fail(format!("error {e}")),
                None => return Outcome::Fail("no query".into()),
            },
            Step::Result { mode, table } => {
                let o = match &result {
                    Some(Ok(o)) => o,
                    Some(Err(e)) => return Outcome::Fail(format!("error {e}")),
                    None => return Outcome::Fail("no query".into()),
                };
                let unordered = *mode == Mode::IgnoringListOrder;
                let Some(head) = table.first() else {
                    return Outcome::Fail("no header".into());
                };
                if *head != o.columns {
                    return Outcome::Fail(format!("columns {:?}, expected {head:?}", o.columns));
                }
                let mut want = Vec::new();
                for row in &table[1..] {
                    let mut r = Vec::new();
                    for c in row {
                        match expected(c, unordered) {
                            Some(v) => r.push(v),
                            None => return Outcome::Skip(format!("expected value {c}")),
                        }
                    }
                    want.push(r.join(" | "));
                }
                let mut got: Vec<String> = o
                    .rows
                    .iter()
                    .map(|r| {
                        r.iter()
                            .map(|v| fmt(v, unordered))
                            .collect::<Vec<_>>()
                            .join(" | ")
                    })
                    .collect();
                if *mode != Mode::InOrder {
                    want.sort();
                    got.sort();
                }
                if want != got {
                    return Outcome::Fail(format!("rows {got:?}, expected {want:?}"));
                }
            }
            Step::Error(what) => match &result {
                Some(Err(_)) => {}
                Some(Ok(o)) => {
                    return Outcome::Fail(format!("{what}: but {} rows came back", o.rows.len()));
                }
                None => return Outcome::Fail("no query".into()),
            },
            Step::NoSideEffects => {
                let c = counts(&stats);
                if !c.is_empty() {
                    return Outcome::Fail(format!("side effects {c:?}"));
                }
            }
            Step::SideEffects(t) => {
                let mut want: BTreeMap<String, i64> = BTreeMap::new();
                for row in t {
                    if row.len() == 2 && row[0] != "-properties" {
                        let n: i64 = row[1].parse().unwrap_or(0);
                        if n != 0 {
                            want.insert(row[0].clone(), n);
                        }
                    }
                }
                let got: BTreeMap<String, i64> = counts(&stats)
                    .into_iter()
                    .map(|(k, v)| (k.to_owned(), v))
                    .collect();
                if want != got {
                    return Outcome::Fail(format!("side effects {got:?}, expected {want:?}"));
                }
            }
        }
    }
    Outcome::Pass
}

fn features(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<PathBuf> = rd.filter_map(|e| e.ok().map(|e| e.path())).collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            features(&p, out);
        } else if p.extension().is_some_and(|e| e == "feature") {
            out.push(p);
        }
    }
}

#[tokio::test]
async fn opencypher_tck() {
    let Some(dir) = std::env::var("SPRIG_TCK_DIR")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        eprintln!("skipped: SPRIG_TCK_DIR is not set");
        return;
    };
    let mut files = Vec::new();
    features(Path::new(&dir), &mut files);
    let mut report = String::new();
    for shards in [1usize, 4] {
        let (mut pass, mut fail, mut skip) = (0usize, 0usize, 0usize);
        let mut by_area: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        for f in &files {
            let area = f
                .strip_prefix(&dir)
                .ok()
                .and_then(|p| {
                    p.iter()
                        .take(2)
                        .map(|x| x.to_string_lossy().into_owned())
                        .reduce(|a, b| format!("{a}/{b}"))
                })
                .unwrap_or_default();
            for s in parse_feature(f) {
                let o = tokio::time::timeout(std::time::Duration::from_secs(20), run(&s, shards))
                    .await
                    .unwrap_or_else(|_| Outcome::Fail("timed out".into()));
                let e = by_area.entry(area.clone()).or_default();
                match o {
                    Outcome::Pass => {
                        pass += 1;
                        e.0 += 1;
                        e.1 += 1;
                    }
                    Outcome::Fail(why) => {
                        fail += 1;
                        e.1 += 1;
                        if shards == 1 {
                            let _ = writeln!(report, "FAIL {} :: {}: {why}", s.feature, s.name);
                        }
                    }
                    Outcome::Skip(why) => {
                        skip += 1;
                        if shards == 1 {
                            let _ = writeln!(report, "SKIP {} :: {}: {why}", s.feature, s.name);
                        }
                    }
                }
            }
        }
        let run = pass + fail;
        eprintln!(
            "openCypher TCK on {shards} shard(s): {pass} of {run} run scenarios pass ({:.1}%), {skip} skipped, {} total",
            100.0 * pass as f64 / run.max(1) as f64,
            run + skip
        );
        if shards == 1 {
            for (a, (p, n)) in &by_area {
                eprintln!(
                    "  {a:<40} {p:>5} / {n:<5} ({:.0}%)",
                    100.0 * *p as f64 / (*n).max(1) as f64
                );
            }
        }
    }
    if let Ok(path) = std::env::var("SPRIG_TCK_REPORT") {
        let _ = std::fs::write(path, report);
    }
}
