//! Evaluation sets and cases, as the TOML files in `evals/` hold them,
//! and the static checks of a set.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

use crate::egress::NameKind;
use crate::role::Role;
use crate::tools::{Column, ColumnStats, Effect, ResultSet, Source, ToolBody, ToolRegistry};

/// One evaluation set: the cases of one role.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Set {
    /// The assistant the cases are for.
    pub role: Role,
    /// What the set covers.
    #[serde(default)]
    pub description: String,
    /// The cases.
    #[serde(rename = "case")]
    pub cases: Vec<Case>,
}

/// One earlier exchange of a multi-turn case.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exchange {
    /// What the user said.
    pub user: String,
    /// What the assistant answered.
    pub assistant: String,
}

/// One glossary entry offered to the assistant.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlossaryEntry {
    /// The term.
    pub term: String,
    /// What it means for this tenant.
    pub definition: String,
    /// Its SQL, if it has one.
    #[serde(default)]
    pub sql: Option<String>,
}

/// One answer of the fake tool world.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldEntry {
    /// The tool.
    pub tool: String,
    /// Serve this entry only when the call's input holds these keys with
    /// these values (strings compared without case). Entries are tried in
    /// order; one without `when` is the tool's fallback.
    #[serde(default)]
    pub when: Option<Value>,
    /// A JSON result (a schema, a plan, documentation, settings). A string
    /// that starts with `{` or `[` is read as JSON text.
    #[serde(default)]
    pub json: Option<Value>,
    /// Rows: `{columns = [["name", "type"], ...], rows = [[...]],
    /// row_count = n}`.
    #[serde(default)]
    pub rows: Option<Value>,
    /// Column statistics.
    #[serde(default)]
    pub stats: Option<Vec<ColumnStats>>,
    /// The tool fails with this message.
    #[serde(default)]
    pub error: Option<String>,
    /// What the result was read from.
    #[serde(default)]
    pub sources: Vec<Source>,
}

/// A tool call the case expects.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedCall {
    /// The tool.
    pub tool: String,
    /// The whole input. The stub sends exactly this; a live run is scored
    /// on its key arguments (see [`key_args`]).
    pub args: Value,
    /// The tool round it belongs to; calls in one round are made side by
    /// side. When absent, the round after the previous call's.
    #[serde(default)]
    pub round: Option<u32>,
    /// Free-text arguments a live call must contain, each a list of
    /// substrings (case doesn't matter): `has = {sql = ["customer_id"]}`.
    #[serde(default)]
    pub has: BTreeMap<String, Vec<String>>,
    /// A live run isn't marked down for leaving it out.
    #[serde(default)]
    pub optional: bool,
}

/// Limits a case runs under, beyond the runner's defaults.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// `ai.max_tool_rounds`.
    #[serde(default)]
    pub max_tool_rounds: Option<u32>,
    /// `ai.budget.user_tokens_day`.
    #[serde(default)]
    pub user_tokens_day: Option<u64>,
}

/// One case.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    /// Unique in its set: `<role>-<nn>`.
    pub id: String,
    /// Groups for the summary (`tuning`, `injection`, ...).
    #[serde(default)]
    pub tags: Vec<String>,
    /// The user's text (for the administration roles, the intake as the
    /// controller writes it).
    pub message: String,
    /// What the surface adds before the text: context chips, the open
    /// database, the editor's query.
    #[serde(default)]
    pub context: Option<String>,
    /// Earlier exchanges in the conversation.
    #[serde(default)]
    pub history: Vec<Exchange>,
    /// The tenant's glossary, as the system prompt carries it.
    #[serde(default)]
    pub glossary: Vec<GlossaryEntry>,
    /// `ai.share_data`.
    #[serde(default)]
    pub share_data: bool,
    /// Names the administration scrubber knows, by kind (`tenant`,
    /// `database`, `table`, `column`, `user`, `host`): none of them may
    /// reach the provider.
    #[serde(default)]
    pub scrub: BTreeMap<String, Vec<String>>,
    /// What each tool returns.
    #[serde(default)]
    pub world: Vec<WorldEntry>,
    /// The tool calls a good answer makes.
    #[serde(default)]
    pub calls: Vec<ExpectedCall>,
    /// Tools that must not be called.
    #[serde(default)]
    pub forbid_tools: Vec<String>,
    /// Phrases that must not be in the answer (safety: a failure zeroes
    /// the case).
    #[serde(default)]
    pub forbid_text: Vec<String>,
    /// Strings that must never reach the provider.
    #[serde(default)]
    pub leak: Vec<String>,
    /// Limits for this case.
    #[serde(default)]
    pub limits: Limits,
    /// How the turn ends: `end_turn` (the default) or `max_tool_rounds`.
    #[serde(default)]
    pub stop: Option<String>,
    /// The turn fails with this error code instead (39992, 39996, ...).
    #[serde(default)]
    pub error: Option<u32>,
    /// The reference answer: what the stub model says at the end.
    #[serde(default)]
    pub answer: Option<String>,
    /// Triage: `noise` or `needs_advice`, the answer's first word.
    #[serde(default)]
    pub label: Option<String>,
    /// Substrings the answer must contain (case doesn't matter).
    #[serde(default)]
    pub must: Vec<String>,
    /// Groups of substrings: the answer contains at least one of each.
    #[serde(default)]
    pub must_any: Vec<Vec<String>>,
    /// Substrings the answer must not contain (quality, not safety).
    #[serde(default)]
    pub must_not: Vec<String>,
    /// The answer cites a source a tool returned.
    #[serde(default)]
    pub cite: bool,
    /// What a good answer does, for a judge model and for people.
    pub rubric: String,
}

/// Free-text arguments: compared in live runs only through `has`.
pub const FREE_TEXT: &[&str] = &[
    "sql",
    "why",
    "estimate",
    "body",
    "definition",
    "rules",
    "chart",
    "query",
    "source",
    "title",
    "subject",
    "kind",
    "term",
    "dashboard",
    "as_user",
];

/// The arguments a live call must match exactly: strings that aren't
/// free text, and lists of strings (the call's list holds all of them).
/// Numbers and booleans are left to the model.
#[must_use]
pub fn key_args(args: &Value) -> Vec<(&str, &Value)> {
    args.as_object()
        .map(|m| {
            m.iter()
                .filter(|(k, v)| {
                    !FREE_TEXT.contains(&k.as_str())
                        && (v.is_string()
                            || v.as_array().is_some_and(|a| a.iter().all(Value::is_string)))
                })
                .map(|(k, v)| (k.as_str(), v))
                .collect()
        })
        .unwrap_or_default()
}

/// A string that holds JSON text, read as JSON; anything else as it is.
#[must_use]
pub fn jsonish(v: &Value) -> Value {
    if let Value::String(s) = v {
        let t = s.trim_start();
        if (t.starts_with('{') || t.starts_with('['))
            && let Ok(j) = serde_json::from_str(s)
        {
            return j;
        }
    }
    v.clone()
}

/// The scrubber's kind named `s`.
#[must_use]
pub fn name_kind(s: &str) -> Option<NameKind> {
    Some(match s {
        "tenant" => NameKind::Tenant,
        "database" => NameKind::Database,
        "table" => NameKind::Table,
        "column" => NameKind::Column,
        "user" => NameKind::User,
        "host" => NameKind::Host,
        _ => return None,
    })
}

impl WorldEntry {
    /// The tool's result.
    ///
    /// # Errors
    /// The entry's `error`, or what is wrong with it.
    pub fn body(&self) -> Result<ToolBody, String> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        if let Some(j) = &self.json {
            return Ok(ToolBody::Json(jsonish(j)));
        }
        if let Some(st) = &self.stats {
            return Ok(ToolBody::Stats(st.clone()));
        }
        if let Some(r) = &self.rows {
            return rows(&jsonish(r)).map(ToolBody::Rows);
        }
        Err(format!(
            "the world entry for {} has no json, rows, stats or error",
            self.tool
        ))
    }

    /// Whether this entry answers `input`.
    #[must_use]
    pub fn serves(&self, input: &Value) -> bool {
        let Some(Value::Object(w)) = &self.when else {
            return true;
        };
        w.iter().all(|(k, v)| match (input.get(k), v) {
            (Some(Value::String(a)), Value::String(b)) => a.eq_ignore_ascii_case(b),
            (Some(a), b) => a == b,
            (None, _) => false,
        })
    }
}

fn rows(v: &Value) -> Result<ResultSet, String> {
    let cols = v
        .get("columns")
        .and_then(Value::as_array)
        .ok_or("rows need columns")?;
    let columns = cols
        .iter()
        .map(|c| {
            let pair = c.as_array().filter(|a| a.len() == 2);
            match pair.map(|a| (a[0].as_str(), a[1].as_str())) {
                Some((Some(n), Some(t))) => Ok(Column {
                    name: n.to_owned(),
                    ty: t.to_owned(),
                }),
                _ => Err("each column is [name, type]".to_owned()),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let data: Vec<Vec<Value>> = v
        .get("rows")
        .and_then(Value::as_array)
        .map(|r| {
            r.iter()
                .map(|x| x.as_array().cloned().unwrap_or_default())
                .collect()
        })
        .unwrap_or_default();
    if data.iter().any(|r| r.len() != columns.len()) {
        return Err("a row's length differs from the columns'".to_owned());
    }
    let row_count = v
        .get("row_count")
        .and_then(Value::as_u64)
        .unwrap_or(data.len() as u64);
    Ok(ResultSet {
        truncated: row_count > data.len() as u64,
        columns,
        rows: data,
        row_count,
    })
}

impl Case {
    /// The user's text as the turn receives it: the context first.
    #[must_use]
    pub fn user_text(&self) -> String {
        match &self.context {
            Some(c) if !c.trim().is_empty() => format!("Context:\n{}\n\n{}", c.trim(), self.message),
            _ => self.message.clone(),
        }
    }

    /// The reference answer: `answer`, or the triage label.
    #[must_use]
    pub fn reference(&self) -> String {
        self.answer
            .clone()
            .or_else(|| self.label.clone())
            .unwrap_or_default()
    }

    /// The expected calls by round, rounds in order.
    #[must_use]
    pub fn rounds(&self) -> Vec<Vec<&ExpectedCall>> {
        let mut out: Vec<(u32, Vec<&ExpectedCall>)> = Vec::new();
        let mut prev = 0;
        for c in &self.calls {
            let r = c.round.unwrap_or(prev + 1);
            match out.last_mut() {
                Some((n, v)) if *n == r => v.push(c),
                _ => out.push((r, vec![c])),
            }
            prev = r;
        }
        out.into_iter().map(|(_, v)| v).collect()
    }

    /// The expected stop label.
    #[must_use]
    pub fn expected_stop(&self) -> &str {
        self.stop.as_deref().unwrap_or("end_turn")
    }

    /// Everything the user (or the surface) supplied: text the model may
    /// legitimately repeat.
    #[must_use]
    pub fn supplied_text(&self) -> String {
        let mut s = self.user_text();
        for h in &self.history {
            s.push('\n');
            s.push_str(&h.user);
            s.push('\n');
            s.push_str(&h.assistant);
        }
        for g in &self.glossary {
            s.push('\n');
            s.push_str(&g.term);
            s.push('\n');
            s.push_str(&g.definition);
            if let Some(q) = &g.sql {
                s.push('\n');
                s.push_str(q);
            }
        }
        for w in &self.world {
            if let Some(j) = &w.json {
                s.push('\n');
                s.push_str(&jsonish(j).to_string());
            }
            if let Some(r) = &w.rows {
                // Column names are schema.
                if let Ok(rs) = rows(&jsonish(r)) {
                    for c in rs.columns {
                        s.push('\n');
                        s.push_str(&c.name);
                    }
                }
            }
            for src in &w.sources {
                s.push('\n');
                s.push_str(&src.title);
                s.push('\n');
                s.push_str(&src.reference);
            }
        }
        s
    }

    /// The names the scrubber knows.
    #[must_use]
    pub fn scrub_names(&self) -> Vec<(NameKind, &str)> {
        self.scrub
            .iter()
            .filter_map(|(k, v)| name_kind(k).map(|k| (k, v)))
            .flat_map(|(k, v)| v.iter().map(move |n| (k, n.as_str())))
            .collect()
    }

    /// Row and value strings that must not reach the provider: with
    /// sharing off in a tenant role, every cell, minimum, maximum and
    /// common value of the world that the user didn't supply themselves.
    /// Short strings and plain integers are left out (they recur by
    /// chance).
    #[must_use]
    pub fn withheld_values(&self, role: Role) -> Vec<String> {
        if !role.is_tenant() || self.share_data {
            return Vec::new();
        }
        let supplied = self.supplied_text().to_lowercase();
        let mut out = BTreeSet::new();
        let mut add = |v: &Value| {
            let s = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) if n.is_f64() => n.to_string(),
                _ => return,
            };
            if s.chars().count() >= 4 && !supplied.contains(&s.to_lowercase()) {
                out.insert(s);
            }
        };
        for w in &self.world {
            if let Some(r) = &w.rows
                && let Ok(rs) = rows(&jsonish(r))
            {
                rs.rows.iter().flatten().for_each(&mut add);
            }
            for st in w.stats.iter().flatten() {
                st.min.iter().chain(st.max.iter()).for_each(&mut add);
                st.top.iter().for_each(|(v, _)| add(v));
            }
        }
        out.into_iter().collect()
    }

    /// Every string that must not reach the provider.
    #[must_use]
    pub fn must_not_send(&self, role: Role) -> Vec<String> {
        let mut v: Vec<String> = self.leak.clone();
        v.extend(self.scrub_names().into_iter().map(|(_, n)| n.to_owned()));
        v.extend(self.withheld_values(role));
        v.sort();
        v.dedup();
        v
    }
}

/// Checks a set: ids unique, every tool named exists and is the role's,
/// every expected input passes the registry's checks, every expected read
/// has a world entry, the reference answer passes the deterministic
/// rubric, and no forbidden string is in what the stub model says.
///
/// # Errors
/// Every problem found, one per line.
pub fn check_set(set: &Set, reg: &ToolRegistry) -> Result<(), Vec<String>> {
    let role = set.role;
    let mut errs = Vec::new();
    let mut ids = BTreeSet::new();
    for c in &set.cases {
        let id = &c.id;
        let mut err = |m: String| errs.push(format!("{id}: {m}"));
        if !ids.insert(c.id.clone()) {
            err("duplicate id".into());
        }
        if !c.id.starts_with(&format!("{role}-")) {
            err(format!("ids start with {role}-"));
        }
        if c.rubric.trim().is_empty() {
            err("no rubric".into());
        }
        if c.message.trim().is_empty() {
            err("no message".into());
        }
        if role == Role::Triage {
            match c.label.as_deref() {
                Some("noise" | "needs_advice") => {}
                _ => err("triage cases need label = noise or needs_advice".into()),
            }
        } else if c.answer.as_deref().is_none_or(|a| a.trim().is_empty()) && c.error.is_none() {
            err("no reference answer".into());
        }
        for (k, _) in &c.scrub {
            if name_kind(k).is_none() {
                err(format!("unknown scrub kind {k}"));
            }
        }
        if !c.scrub.is_empty() && role.is_tenant() {
            err("scrub is for the administration roles".into());
        }
        if let Some(s) = &c.stop
            && !matches!(s.as_str(), "end_turn" | "max_tool_rounds")
        {
            err(format!("unknown stop {s}"));
        }
        let mut prev = 0;
        for call in &c.calls {
            let r = call.round.unwrap_or(prev + 1);
            if r < prev {
                err(format!("{}: rounds go backwards", call.tool));
            }
            prev = r;
            match reg.check(role, &call.tool, &call.args) {
                Ok(spec) => {
                    if spec.effect == Effect::Read
                        && !c.world.iter().any(|w| w.tool == call.tool && w.serves(&call.args))
                    {
                        err(format!("{} has no world entry for its args", call.tool));
                    }
                }
                Err(e) => err(format!("{}: {}", call.tool, e.message)),
            }
            for k in call.has.keys() {
                if call.args.get(k).is_none() {
                    err(format!("{}: has.{k} names no argument", call.tool));
                }
            }
            for (k, subs) in &call.has {
                let v = call.args.get(k).map(value_text).unwrap_or_default();
                for s in subs {
                    if !v.to_lowercase().contains(&s.to_lowercase()) {
                        err(format!("{}: args.{k} lacks its own has {s:?}", call.tool));
                    }
                }
            }
        }
        for w in &c.world {
            match reg.get(&w.tool) {
                Some(s) if s.allows(role) && s.effect == Effect::Read => {}
                Some(_) => err(format!("world tool {} isn't a read tool of {role}", w.tool)),
                None => err(format!("world tool {} doesn't exist", w.tool)),
            }
            if let Err(e) = w.body().or_else(|e| {
                if w.error.is_some() {
                    Ok(ToolBody::Json(Value::Null))
                } else {
                    Err(e)
                }
            }) {
                err(e);
            }
        }
        for t in &c.forbid_tools {
            if reg.get(t).is_none() {
                err(format!("forbidden tool {t} doesn't exist"));
            }
            if c.calls.iter().any(|x| &x.tool == t) {
                err(format!("{t} is both expected and forbidden"));
            }
        }
        // The stub's calls are sent back to the provider in the next
        // request, so they must hold nothing the gate keeps back.
        let args: String = c
            .calls
            .iter()
            .map(|x| x.args.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        for term in c.must_not_send(role) {
            if contains_word(&args, &term) {
                err(format!("the expected calls hold {term:?}, which must not be sent"));
            }
        }
        if !role.is_tenant() {
            for (_, n) in c.scrub_names() {
                if contains_word(&c.reference(), n) {
                    err(format!("the reference answer names {n:?}, which the gate hides"));
                }
            }
        }
    }
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

/// A JSON value as text: strings bare, the rest as JSON.
#[must_use]
pub fn value_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Whether `hay` holds `term` as a whole word, case aside.
#[must_use]
pub fn contains_word(hay: &str, term: &str) -> bool {
    let h = hay.to_lowercase();
    let t = term.to_lowercase();
    if t.is_empty() {
        return false;
    }
    let hb = h.as_bytes();
    let first = t.as_bytes()[0];
    let last = t.as_bytes()[t.len() - 1];
    let mut from = 0;
    while let Some(p) = h[from..].find(&t) {
        let at = from + p;
        let end = at + t.len();
        let before = at == 0 || !word_byte(first) || !word_byte(hb[at - 1]);
        let after = end >= hb.len() || !word_byte(last) || !word_byte(hb[end]);
        if before && after {
            return true;
        }
        from = at + t.len().max(1);
        while from < h.len() && !h.is_char_boundary(from) {
            from += 1;
        }
    }
    false
}
