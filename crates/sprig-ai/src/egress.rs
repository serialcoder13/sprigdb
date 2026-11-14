//! The egress gate: what may leave SprigDB for a provider.
//!
//! | Role | Sent | Removed or reduced |
//! |---|---|---|
//! | admin, triage | codes, components, spans, counts, versions, metric series | names of tenants, databases, tables and users, statement text, values, addresses: replaced by stable placeholders (`table#kqzmfa`) |
//! | dba, data, sharing off | schema, plans, statistics without values, the user's text | rows (reduced to columns, types and a row count), minimum, maximum and common values |
//! | dba, data, sharing on | the above, plus results: 200 rows and 64 KiB per tool result | secrets |
//!
//! - **Secrets** are masked in everything the user types and every tool
//!   result: strings after `IDENTIFIED BY`, `PASSWORD`, `SECRET`, `TOKEN`
//!   and the like, words that look like API keys or session tokens, and
//!   `Bearer` credentials.
//! - **Untrusted.** Every tool result is wrapped in
//!   `<tool_result untrusted="true">…</tool_result>`, and the system prompt
//!   says text inside is data, never instructions. A closing tag inside a
//!   result is escaped, so stored text can't end the wrapper early.
//! - **Append-only.** Each message is gated once, when it joins the
//!   conversation; the gate is deterministic and idempotent, so checking
//!   the whole request again before it leaves changes nothing, and the
//!   provider sees the same bytes for the same history.

use std::sync::Arc;

use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sprig_common::{Result, SprigError};

use crate::provider::{Block, ChatRequest, MsgRole};
use crate::role::Role;
use crate::tools::{ColumnStats, ResultSet, ToolBody, ToolOutput, ToolSpec};

/// Rows a shared result may carry.
pub const MAX_RESULT_ROWS: usize = 200;
/// Bytes one tool result may carry.
pub const MAX_RESULT_BYTES: usize = 64 * 1024;

const OPEN: &str = "<tool_result untrusted=\"true\">\n";
const CLOSE: &str = "\n</tool_result>";

/// What a role may send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressPolicy {
    /// The assistant's role.
    pub role: Role,
    /// `ai.share_data` for the tenant (and database); never on for the
    /// administration roles.
    pub share_data: bool,
}

impl EgressPolicy {
    /// Whether rows and values may be sent.
    #[must_use]
    pub const fn rows_allowed(&self) -> bool {
        self.share_data && self.role.is_tenant()
    }

    /// `Ok` when rows may be sent; else 39995 naming `what` (a context
    /// chip holding a result, an AI function's input).
    ///
    /// # Errors
    /// 39995.
    pub fn require_rows(&self, what: &str) -> Result<()> {
        if self.rows_allowed() {
            Ok(())
        } else {
            Err(crate::error::egress_denied(what))
        }
    }
}

/// A tool result after the gate.
#[derive(Debug, Clone, PartialEq)]
pub struct Gated {
    /// What the model gets: the wrapped JSON.
    pub content: String,
    /// Something was withheld (rows, values) or cut.
    pub reduced: bool,
    /// The JSON inside the wrapper: what is stored with the conversation.
    pub stored: Value,
}

/// The gate for one conversation.
#[derive(Debug, Clone)]
pub struct EgressGate {
    policy: EgressPolicy,
    scrubber: Option<Arc<Scrubber>>,
}

impl EgressGate {
    /// The gate for `policy`. The administration roles always scrub: with
    /// `scrubber` when given (its key keeps placeholders stable across
    /// requests), else with a fresh key.
    #[must_use]
    pub fn new(mut policy: EgressPolicy, scrubber: Option<Arc<Scrubber>>) -> Self {
        if !policy.role.is_tenant() {
            policy.share_data = false;
        }
        let scrubber = if policy.role.is_tenant() {
            None
        } else {
            Some(scrubber.unwrap_or_else(|| Arc::new(Scrubber::random())))
        };
        Self { policy, scrubber }
    }

    /// The policy.
    #[must_use]
    pub const fn policy(&self) -> &EgressPolicy {
        &self.policy
    }

    /// Text the user (or a context chip) adds: secrets masked; scrubbed
    /// for the administration roles.
    #[must_use]
    pub fn user_text(&self, text: &str) -> String {
        let t = redact_secrets(text);
        match &self.scrubber {
            Some(s) => s.text(&t),
            None => t,
        }
    }

    fn string(&self, s: &str) -> String {
        self.user_text(s)
    }

    fn strings(&self, v: &Value) -> Value {
        match v {
            Value::String(s) => Value::String(self.string(s)),
            Value::Array(a) => Value::Array(a.iter().map(|x| self.strings(x)).collect()),
            Value::Object(m) => Value::Object(
                m.iter()
                    .map(|(k, x)| (self.string(k), self.strings(x)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    /// A read tool's result, reduced as the policy says, then wrapped.
    #[must_use]
    pub fn tool_result(&self, spec: &ToolSpec, out: &ToolOutput) -> Gated {
        let (mut stored, mut reduced) = match &out.body {
            ToolBody::Json(v) => (self.strings(v), false),
            ToolBody::Rows(rs) => self.rows(spec, rs),
            ToolBody::Stats(st) => self.stats(st),
        };
        let mut text = stored.to_string();
        if text.len() > MAX_RESULT_BYTES {
            let mut cut = MAX_RESULT_BYTES;
            while !text.is_char_boundary(cut) {
                cut -= 1;
            }
            text.truncate(cut);
            stored = json!({"truncated": true, "partial": text});
            text = stored.to_string();
            reduced = true;
        }
        Gated {
            content: wrap(&text),
            reduced,
            stored,
        }
    }

    fn rows(&self, spec: &ToolSpec, rs: &ResultSet) -> (Value, bool) {
        let columns: Vec<Value> = rs
            .columns
            .iter()
            .map(|c| json!({"name": self.string(&c.name), "type": c.ty}))
            .collect();
        if !self.policy.rows_allowed() {
            let why = if self.policy.role.is_tenant() {
                format!(
                    "Data sharing is off (ai.share_data), so the rows weren't sent. Tell the user what the query shows in general, and offer it with propose_sql so they can run {} themselves.",
                    spec.name
                )
            } else {
                "Rows are never sent for this assistant.".to_owned()
            };
            return (
                json!({"columns": columns, "row_count": rs.row_count, "rows_withheld": why}),
                true,
            );
        }
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        let mut cut = rs.truncated;
        for r in &rs.rows {
            if rows.len() >= MAX_RESULT_ROWS {
                cut = true;
                break;
            }
            let row = self.strings(&Value::Array(r.clone()));
            bytes += row.to_string().len() + 1;
            if bytes > MAX_RESULT_BYTES - 1024 {
                cut = true;
                break;
            }
            rows.push(row);
        }
        let reduced = cut && rows.len() < rs.rows.len();
        let mut m = Map::new();
        m.insert("columns".into(), Value::Array(columns));
        m.insert("row_count".into(), json!(rs.row_count));
        m.insert("rows".into(), Value::Array(rows));
        if cut {
            m.insert("truncated".into(), json!(true));
        }
        (Value::Object(m), reduced)
    }

    fn stats(&self, st: &[ColumnStats]) -> (Value, bool) {
        let values = self.policy.rows_allowed();
        let mut reduced = false;
        let cols: Vec<Value> = st
            .iter()
            .map(|c| {
                let mut v = json!({
                    "column": self.string(&c.column),
                    "type": c.ty,
                    "count": c.count,
                    "nulls": c.nulls,
                    "distinct": c.distinct,
                });
                let has = c.min.is_some() || c.max.is_some() || !c.top.is_empty();
                if values {
                    v["min"] = self.strings(&c.min.clone().unwrap_or(Value::Null));
                    v["max"] = self.strings(&c.max.clone().unwrap_or(Value::Null));
                    v["top"] = Value::Array(
                        c.top
                            .iter()
                            .map(|(x, n)| json!([self.strings(x), n]))
                            .collect(),
                    );
                } else if has {
                    reduced = true;
                }
                v
            })
            .collect();
        let mut out = json!({"columns": cols});
        if reduced {
            out["values_withheld"] = json!(
                "Data sharing is off (ai.share_data): minimum, maximum and common values weren't sent."
            );
        }
        (out, reduced)
    }

    /// A tool error, wrapped like a result.
    #[must_use]
    pub fn tool_error(&self, message: &str) -> String {
        wrap(&json!({"error": self.string(message)}).to_string())
    }

    /// The last check before a request leaves: every user text gated
    /// (a no-op for text already gated), every tool result wrapped.
    ///
    /// # Errors
    /// Internal for a tool result that didn't pass the gate (a bug).
    pub fn check(&self, req: &mut ChatRequest) -> Result<()> {
        for m in &mut req.messages {
            if m.role != MsgRole::User {
                continue;
            }
            for b in &mut m.content {
                match b {
                    Block::Text { text } => {
                        let g = self.user_text(text);
                        if g != *text {
                            *text = g;
                        }
                    }
                    Block::ToolResult { content, .. } => {
                        if !(content.starts_with(OPEN) && content.ends_with(CLOSE)) {
                            return Err(SprigError::internal(
                                "a tool result skipped the egress gate",
                            ));
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

/// `json` inside the untrusted wrapper, with any closing tag inside it
/// escaped.
#[must_use]
pub fn wrap(json: &str) -> String {
    format!(
        "{OPEN}{}{CLOSE}",
        json.replace("</tool_result", "<\\/tool_result")
    )
}

// ---- secrets ----

/// Words after which the next quoted string is a secret.
const SECRET_WORDS: &[&str] = &[
    "BY",
    "PASSWORD",
    "PASSWD",
    "SECRET",
    "TOKEN",
    "CREDENTIALS",
    "API_KEY",
    "APIKEY",
    "ACCESS_KEY",
    "SECRET_KEY",
    "SASL_PASSWORD",
    "PWD",
    "AUTH",
];

/// Prefixes of keys and tokens: SprigDB's own and common providers'.
const TOKEN_PREFIXES: &[&str] = &[
    "sprig_",
    "sprigat_",
    "sprigrt_",
    "sk-",
    "ghp_",
    "gho_",
    "github_pat_",
    "xoxb-",
    "xoxp-",
    "AKIA",
    "ASIA",
    "glpat-",
    "AIza",
];

fn looks_like_token(w: &str) -> bool {
    // A SprigDB API key is `sprig_<tenant>_<key id>_<64 hex digits>`; other
    // `sprig_` words, such as metric names, aren't secrets.
    if let Some(rest) = w.strip_prefix("sprig_") {
        let secret = rest.rsplit('_').next().unwrap_or("");
        return rest.contains('_')
            && secret.len() >= 32
            && secret.bytes().all(|b| b.is_ascii_hexdigit());
    }
    TOKEN_PREFIXES
        .iter()
        .any(|p| w.starts_with(p) && w.len() >= p.len() + 12)
}

fn word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '$')
}

/// The end (exclusive) of the quoted literal opening at byte `start`,
/// with doubled quotes and (except in backticks) backslash escapes; `None`
/// when it isn't closed.
fn literal_end(text: &str, start: usize) -> Option<usize> {
    let b = text.as_bytes();
    let q = b[start];
    let mut i = start + 1;
    while i < b.len() {
        if b[i] == b'\\' && q != b'`' {
            i += 2;
        } else if b[i] == q {
            if b.get(i + 1) == Some(&q) {
                i += 2;
            } else {
                return Some(i + 1);
            }
        } else {
            i += 1;
        }
    }
    None
}

/// Whether the quote at byte `i` opens a literal: not an apostrophe inside
/// a word (`don't`).
fn opens_literal(text: &str, i: usize) -> bool {
    let b = text.as_bytes();
    !(b[i] == b'\'' && i > 0 && b[i - 1].is_ascii_alphanumeric())
}

/// `text` with its secrets masked: quoted strings after a secret word
/// become `'***'`, key-like words and `Bearer` credentials `***`.
/// Idempotent.
#[must_use]
pub fn redact_secrets(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut armed = false;
    let mut bearer = false;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if (c == b'\'' || c == b'"') && opens_literal(text, i) {
            if let Some(end) = literal_end(text, i) {
                let lit = &text[i..end];
                let inner = &lit[1..lit.len() - 1];
                if armed || looks_like_token(inner) {
                    out.push(char::from(c));
                    out.push_str("***");
                    out.push(char::from(c));
                } else {
                    out.push_str(lit);
                }
                armed = false;
                bearer = false;
                i = end;
                continue;
            }
            out.push(char::from(c));
            i += 1;
            continue;
        }
        let ch = text[i..].chars().next().unwrap_or(' ');
        if word_char(ch) {
            let mut end = i;
            for (k, d) in text[i..].char_indices() {
                if word_char(d) || (d == '.' && bearer) {
                    end = i + k + d.len_utf8();
                } else {
                    break;
                }
            }
            let w = &text[i..end];
            if bearer && w != "***" {
                out.push_str("***");
                bearer = false;
                armed = false;
            } else if looks_like_token(w) {
                out.push_str("***");
                armed = false;
            } else {
                let up = w.to_ascii_uppercase();
                bearer = up == "BEARER";
                armed = SECRET_WORDS.contains(&up.as_str())
                    || (armed && matches!(up.as_str(), "USING" | "WITH" | "AS" | "IS"));
                out.push_str(w);
            }
            i = end;
        } else {
            if !(ch.is_whitespace() || matches!(ch, '=' | ':' | '(')) {
                armed = false;
                bearer = false;
            }
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

// ---- the administration scrubber ----

/// What a placeholder stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NameKind {
    /// A tenant.
    Tenant,
    /// A database.
    Database,
    /// A table or collection.
    Table,
    /// A column.
    Column,
    /// A user.
    User,
    /// A host name.
    Host,
    /// A value.
    Value,
    /// Statement text.
    Statement,
    /// An address: IP, host and port, URL, email.
    Address,
    /// A quoted identifier.
    Name,
}

impl NameKind {
    /// The placeholder's prefix.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            NameKind::Tenant => "tenant",
            NameKind::Database => "database",
            NameKind::Table => "table",
            NameKind::Column => "column",
            NameKind::User => "user",
            NameKind::Host => "host",
            NameKind::Value => "value",
            NameKind::Statement => "statement",
            NameKind::Address => "address",
            NameKind::Name => "name",
        }
    }
}

const LABELS: &[&str] = &[
    "tenant",
    "database",
    "table",
    "column",
    "user",
    "host",
    "value",
    "statement",
    "address",
    "name",
];

/// Words that start a statement.
const STATEMENT_START: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "ALTER", "DROP", "TRUNCATE",
    "GRANT", "REVOKE", "CALL", "WITH", "SHOW", "LOAD", "MERGE", "UPSERT",
];
/// Words that, later on the same line, confirm a statement.
const STATEMENT_MORE: &[&str] = &[
    "FROM", "INTO", "SET", "TABLE", "WHERE", "VALUES", "DATABASE", "SCHEMA", "INDEX", "VIEW",
    "USER", "ON", "BY", "AS", "JOIN", "TO",
];
/// The confirming words that are rare in English prose. A statement
/// written in lower case (`show ...`, `with ...`) needs one of these, so
/// "show the lag on the replica" or "with luck it moves to c2" stays text.
const STATEMENT_STRONG: &[&str] = &[
    "FROM",
    "INTO",
    "WHERE",
    "VALUES",
    "TABLE",
    "TABLES",
    "DATABASE",
    "DATABASES",
    "SCHEMA",
    "INDEX",
    "VIEW",
    "JOIN",
    "SELECT",
];

/// Digits in a run that is treated as a value (or an id) rather than a
/// count.
const VALUE_DIGITS: usize = 7;

/// Replaces names, values, statements and addresses with stable
/// placeholders, for what the administration agent sends.
///
/// A placeholder is `<kind>#` and six letters derived from the name with
/// HMAC-SHA256 under the scrubber's key, so the same name gives the same
/// placeholder for as long as the key lasts, and nothing can be read back
/// from it.
pub struct Scrubber {
    key: hmac::Key,
    names: Vec<(String, NameKind)>,
}

impl std::fmt::Debug for Scrubber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scrubber")
            .field("names", &self.names.len())
            .finish_non_exhaustive()
    }
}

fn token_char(c: char) -> bool {
    c.is_alphanumeric()
        || matches!(
            c,
            '_' | '.' | ':' | '@' | '%' | '-' | '/' | '#' | '$' | '[' | ']'
        )
}

fn is_placeholder(t: &str) -> bool {
    t.split_once('#').is_some_and(|(l, h)| {
        LABELS.contains(&l) && h.len() == 6 && h.bytes().all(|b| b.is_ascii_lowercase())
    })
}

fn is_ipv4(t: &str) -> bool {
    let host = t.rsplit_once(':').map_or(t, |(h, p)| {
        if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) {
            h
        } else {
            t
        }
    });
    let parts: Vec<&str> = host.split('.').collect();
    parts.len() == 4
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.len() <= 3 && p.bytes().all(|b| b.is_ascii_digit()))
}

fn is_ipv6(t: &str) -> bool {
    let t = t.trim_start_matches('[');
    let t = t.split(']').next().unwrap_or(t);
    t.matches(':').count() >= 2
        && t.chars().any(|c| c.is_ascii_hexdigit())
        && t.chars()
            .all(|c| c.is_ascii_hexdigit() || c == ':' || c == '.')
}

fn is_host_port(t: &str) -> bool {
    t.rsplit_once(':').is_some_and(|(h, p)| {
        !p.is_empty()
            && p.len() <= 5
            && p.bytes().all(|b| b.is_ascii_digit())
            && h.contains('.')
            && h.chars().next().is_some_and(char::is_alphanumeric)
    })
}

fn is_email(t: &str) -> bool {
    t.split_once('@')
        .is_some_and(|(u, d)| !u.is_empty() && d.contains('.') && !d.starts_with('.'))
}

fn tenant_schema(t: &str) -> bool {
    let Some(rest) = t.strip_prefix('t') else {
        return false;
    };
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    digits > 0 && (digits == rest.len() || rest.as_bytes()[digits] == b'_')
}

impl Scrubber {
    /// A scrubber with `key`.
    #[must_use]
    pub fn new(key: &[u8; 32]) -> Self {
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, key),
            names: Vec::new(),
        }
    }

    /// A scrubber with a random key: placeholders are stable only within
    /// it.
    #[must_use]
    pub fn random() -> Self {
        let mut k = [0u8; 32];
        if getrandom::fill(&mut k).is_err() {
            // No randomness: placeholders become guessable by name, never
            // reversible; still no name is sent.
            k = [0x5a; 32];
        }
        let s = Self::new(&k);
        zeroize::Zeroize::zeroize(&mut k);
        s
    }

    /// Adds a name to replace wherever it appears as a word or a dotted
    /// part (`orders`, `shop.orders`). Case doesn't matter.
    pub fn add(&mut self, kind: NameKind, name: &str) {
        let n = name.trim().to_lowercase();
        if n.is_empty() || self.names.iter().any(|(x, _)| *x == n) {
            return;
        }
        self.names.push((n, kind));
        self.names
            .sort_by(|a, b| b.0.len().cmp(&a.0.len()).then(a.0.cmp(&b.0)));
    }

    /// The placeholder of `name` as a `kind`.
    #[must_use]
    pub fn placeholder(&self, kind: NameKind, name: &str) -> String {
        let mut msg = Vec::with_capacity(name.len() + 12);
        msg.extend_from_slice(kind.label().as_bytes());
        msg.push(0);
        msg.extend_from_slice(name.to_lowercase().as_bytes());
        let tag = hmac::sign(&self.key, &msg);
        let letters: String = tag.as_ref()[..6]
            .iter()
            .map(|b| char::from(b'a' + b % 26))
            .collect();
        format!("{}#{letters}", kind.label())
    }

    fn registered(&self, word: &str) -> Option<NameKind> {
        let w = word.to_lowercase();
        self.names.iter().find(|(n, _)| *n == w).map(|(_, k)| *k)
    }

    /// `text` with every name, value, statement and address replaced.
    /// Idempotent.
    #[must_use]
    pub fn text(&self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        for (n, line) in text.split('\n').enumerate() {
            if n > 0 {
                out.push('\n');
            }
            let line = line.strip_suffix('\r').map_or_else(
                || self.line(line),
                |l| {
                    let mut s = self.line(l);
                    s.push('\r');
                    s
                },
            );
            out.push_str(&line);
        }
        out
    }

    /// Where a statement starts on `line`, if one does.
    fn statement_at(line: &str) -> Option<usize> {
        let up = line.to_ascii_uppercase();
        let b = up.as_bytes();
        let boundary = |i: usize| i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
        for kw in STATEMENT_START {
            let mut from = 0;
            while let Some(p) = up[from..].find(kw) {
                let at = from + p;
                let end = at + kw.len();
                let after_ok = b
                    .get(end)
                    .is_none_or(|c| !(c.is_ascii_alphanumeric() || *c == b'_'));
                if boundary(at) && after_ok {
                    let rest = &up[end..];
                    // Upper case, as statements are usually written: any
                    // confirming word will do. Otherwise only a strong one.
                    let confirm = if line[at..end] == up[at..end] {
                        STATEMENT_MORE
                    } else {
                        STATEMENT_STRONG
                    };
                    let more = rest
                        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                        .any(|w| confirm.contains(&w));
                    if more {
                        return Some(at);
                    }
                }
                from = end;
            }
        }
        None
    }

    fn line(&self, line: &str) -> String {
        if let Some(at) = Self::statement_at(line) {
            let stmt = line[at..].trim_end();
            let mut s = self.tokens(&line[..at]);
            s.push_str(&self.placeholder(NameKind::Statement, stmt));
            return s;
        }
        self.tokens(line)
    }

    fn tokens(&self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut prev_word = String::new();
        let mut it = text.char_indices().peekable();
        while let Some((i, c)) = it.next() {
            if matches!(c, '\'' | '"' | '`')
                && opens_literal(text, i)
                && let Some(end) = literal_end(text, i)
            {
                while it.peek().is_some_and(|&(j, _)| j < end) {
                    it.next();
                }
                let lit = &text[i..end];
                let kind = if c == '`' {
                    NameKind::Name
                } else {
                    NameKind::Value
                };
                out.push_str(&self.placeholder(kind, lit));
                prev_word.clear();
            } else if token_char(c) {
                let mut end = i + c.len_utf8();
                while let Some(&(j, d)) = it.peek() {
                    if token_char(d) {
                        end = j + d.len_utf8();
                        it.next();
                    } else {
                        break;
                    }
                }
                // A URL runs on to the next space or quote, query included.
                if text[i..end].contains("://") {
                    while let Some(&(j, d)) = it.peek() {
                        if d.is_whitespace()
                            || matches!(d, '\'' | '"' | '`' | '<' | '>' | '(' | ')')
                        {
                            break;
                        }
                        end = j + d.len_utf8();
                        it.next();
                    }
                }
                // Trailing punctuation belongs to the sentence.
                let raw = &text[i..end];
                let tok = raw.trim_end_matches(['.', ':', '-', '/', ']']);
                let tail = &raw[tok.len()..];
                out.push_str(&self.token(tok, &prev_word));
                out.push_str(tail);
                tok.to_lowercase().clone_into(&mut prev_word);
            } else {
                out.push(c);
                if !c.is_whitespace() && c != '=' {
                    prev_word.clear();
                }
            }
        }
        out
    }

    fn token(&self, t: &str, prev: &str) -> String {
        if t.is_empty() || is_placeholder(t) {
            return t.to_owned();
        }
        if t.contains("://") || is_email(t) || is_ipv4(t) || is_ipv6(t) || is_host_port(t) {
            return self.placeholder(NameKind::Address, t);
        }
        if let Some(k) = self.registered(t) {
            return self.placeholder(k, t);
        }
        let digits = t.bytes().all(|b| b.is_ascii_digit());
        if digits && matches!(prev, "tenant" | "tenant_id" | "tid" | "tenant#") {
            return self.placeholder(NameKind::Tenant, t);
        }
        if digits && t.len() >= VALUE_DIGITS {
            return self.placeholder(NameKind::Value, t);
        }
        if tenant_schema(&t.to_lowercase()) {
            return self.placeholder(NameKind::Tenant, t);
        }
        // Parts: `shop.orders`, `alice@db1`, `db1:3306`.
        let mut out = String::with_capacity(t.len());
        let mut start = 0;
        for (k, c) in t.char_indices() {
            if matches!(c, '.' | ':' | '@' | '/') {
                out.push_str(&self.part(&t[start..k]));
                out.push(c);
                start = k + 1;
            }
        }
        out.push_str(&self.part(&t[start..]));
        out
    }

    fn part(&self, p: &str) -> String {
        if p.is_empty() || is_placeholder(p) {
            return p.to_owned();
        }
        if let Some(k) = self.registered(p) {
            return self.placeholder(k, p);
        }
        if tenant_schema(&p.to_lowercase()) {
            return self.placeholder(NameKind::Tenant, p);
        }
        if p.len() >= VALUE_DIGITS && p.bytes().all(|b| b.is_ascii_digit()) {
            return self.placeholder(NameKind::Value, p);
        }
        p.to_owned()
    }
}

#[cfg(test)]
mod tests;
