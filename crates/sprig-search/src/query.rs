//! One query AST for every front end, and its parsers.
//!
//! - [`parse_query_string`]: `sprig_search()` and Atlas `queryString`:
//!   terms (OR by default), `"phrases"` with `~slop`, `+must`, `-not`,
//!   `field:term`, `term~1` (fuzzy), `prefix*`, `term^2` (boost),
//!   parentheses, `AND`, `OR`, `NOT`.
//! - [`parse_mysql_boolean`] and [`mysql_natural`]: `MATCH ... AGAINST`.
//!   `>w`, `<w` and `~w` become boosts 1.5, 0.67 and 0.5.
//! - [`parse_mongo_text`]: `$text`'s `$search` string: terms OR'ed, every
//!   quoted phrase required, `-term` and `-"phrase"` excluded.
//! - [`parse_atlas`]: a `$search` operator (`text`, `phrase`, `compound`,
//!   `autocomplete`, `range`, `equals`, `exists`, `wildcard`, `regex`,
//!   `queryString`).

use std::ops::Bound;

use serde_json::Value as Json;
use sprig_common::{Result, SprigCode, SprigError};

/// A literal of a range or equality.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Lit {
    /// A number.
    Num(f64),
    /// A string (keyword fields; dates as text).
    Str(String),
    /// A boolean.
    Bool(bool),
    /// A date, in ms since the epoch.
    DateMs(i64),
}

/// The query AST.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SearchQuery {
    /// Every document.
    All,
    /// The analyzed text in any of `fields` (empty: the index's default
    /// text fields): any token (`all` false) or every token.
    Match {
        /// Fields by name.
        fields: Vec<String>,
        /// The text.
        text: String,
        /// Every token required.
        all: bool,
        /// Fuzzy distance (1 or 2).
        fuzzy: Option<u8>,
    },
    /// The analyzed tokens in order, with up to `slop` moves.
    Phrase {
        /// Fields by name.
        fields: Vec<String>,
        /// The text.
        text: String,
        /// Allowed moves.
        slop: u32,
    },
    /// Terms starting with the (analyzed) text.
    Prefix {
        /// Fields by name.
        fields: Vec<String>,
        /// The prefix.
        text: String,
    },
    /// A wildcard pattern (`*`, `?`) on raw terms.
    Wildcard {
        /// Fields by name.
        fields: Vec<String>,
        /// The pattern.
        pattern: String,
    },
    /// A regular expression on raw terms.
    Regex {
        /// Fields by name.
        fields: Vec<String>,
        /// The pattern.
        pattern: String,
    },
    /// The field has a value.
    Exists(String),
    /// A range.
    Range {
        /// The field.
        field: String,
        /// Lower bound.
        lo: Bound<Lit>,
        /// Upper bound.
        hi: Bound<Lit>,
    },
    /// Exact value.
    Equals {
        /// The field.
        field: String,
        /// The value.
        value: Lit,
    },
    /// Clauses.
    Bool {
        /// All required (scored).
        must: Vec<SearchQuery>,
        /// Optional (scored); at least `min_should` when there is no `must`.
        should: Vec<SearchQuery>,
        /// None may match.
        must_not: Vec<SearchQuery>,
        /// All required (not scored).
        filter: Vec<SearchQuery>,
        /// Minimum `should` clauses that must match (0: Lucene's default).
        min_should: usize,
    },
    /// A score multiplier.
    Boost(Box<SearchQuery>, f32),
    /// Documents whose encoded key (`_key`) is one of these: the gateway
    /// restricts counts and facets to the rows a caller may see. Never parsed from a client's query.
    Keys(Vec<crate::schema::Key>),
}

impl SearchQuery {
    /// A `Bool` with only `should` clauses.
    #[must_use]
    pub fn any(should: Vec<SearchQuery>) -> Self {
        Self::Bool {
            must: Vec::new(),
            should,
            must_not: Vec::new(),
            filter: Vec::new(),
            min_should: 0,
        }
    }

    /// Every field the query names (for privilege checks).
    #[must_use]
    pub fn fields(&self) -> Vec<String> {
        let mut out = Vec::new();
        self.walk_fields(&mut out);
        out.sort();
        out.dedup();
        out
    }

    fn walk_fields(&self, out: &mut Vec<String>) {
        match self {
            Self::All | Self::Keys(_) => {}
            Self::Match { fields, .. }
            | Self::Phrase { fields, .. }
            | Self::Prefix { fields, .. }
            | Self::Wildcard { fields, .. }
            | Self::Regex { fields, .. } => out.extend(fields.iter().cloned()),
            Self::Exists(f) | Self::Range { field: f, .. } | Self::Equals { field: f, .. } => {
                out.push(f.clone());
            }
            Self::Bool {
                must,
                should,
                must_not,
                filter,
                ..
            } => {
                for q in must.iter().chain(should).chain(must_not).chain(filter) {
                    q.walk_fields(out);
                }
            }
            Self::Boost(q, _) => q.walk_fields(out),
        }
    }
}

fn syntax(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, msg)
}

fn unsupported(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchUnsupported, msg)
}

// ---- Query strings ----

#[derive(Debug, Clone, PartialEq)]
enum Tk {
    Word(String),
    Quoted(String),
    Plus,
    Minus,
    Gt,
    Lt,
    Tilde,
    Open,
    Close,
    Colon,
    Star,
    Caret,
}

/// Splits a query string. `mysql` treats `>`, `<` and a leading `~` as
/// operators and has no `field:` or `^`.
fn lex(s: &str, mysql: bool) -> Result<Vec<Tk>> {
    let mut out = Vec::new();
    let mut it = s.char_indices().peekable();
    let special = |c: char| {
        c.is_whitespace()
            || matches!(c, '"' | '(' | ')' | '*')
            || (!mysql && matches!(c, ':' | '^' | '~'))
            || (mysql && matches!(c, '~'))
    };
    // `+` and `-` are operators at the start of a word only.
    let mut word_start = true;
    while let Some((_, c)) = it.next() {
        let at_start = word_start;
        word_start = c.is_whitespace() || c == '(';
        match c {
            c if c.is_whitespace() => {}
            '"' => {
                let mut q = String::new();
                let mut closed = false;
                for (_, c) in it.by_ref() {
                    if c == '"' {
                        closed = true;
                        break;
                    }
                    q.push(c);
                }
                if !closed && !mysql {
                    return Err(syntax("unterminated quoted phrase in search query"));
                }
                out.push(Tk::Quoted(q));
            }
            '(' => out.push(Tk::Open),
            ')' => out.push(Tk::Close),
            '*' => out.push(Tk::Star),
            '~' => {
                out.push(Tk::Tilde);
                word_start = mysql;
            }
            ':' if !mysql => out.push(Tk::Colon),
            '^' if !mysql => out.push(Tk::Caret),
            '+' if at_start => {
                out.push(Tk::Plus);
                word_start = true;
            }
            '-' if at_start => {
                out.push(Tk::Minus);
                word_start = true;
            }
            '>' if mysql => {
                out.push(Tk::Gt);
                word_start = true;
            }
            '<' if mysql => {
                out.push(Tk::Lt);
                word_start = true;
            }
            c => {
                let mut w = String::from(c);
                while let Some((_, n)) = it.peek() {
                    if special(*n) {
                        break;
                    }
                    w.push(*n);
                    it.next();
                }
                out.push(Tk::Word(w));
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Occ {
    Must,
    Should,
    Not,
}

struct Qs<'a> {
    t: &'a [Tk],
    i: usize,
    fields: Vec<String>,
}

impl Qs<'_> {
    fn peek(&self) -> Option<&Tk> {
        self.t.get(self.i)
    }

    fn word_is(&self, w: &str) -> bool {
        matches!(self.peek(), Some(Tk::Word(x)) if x == w)
    }

    /// clauses until `)` or the end.
    fn clauses(&mut self) -> Result<SearchQuery> {
        let mut items: Vec<(Occ, SearchQuery)> = Vec::new();
        let mut and_next = false;
        loop {
            match self.peek() {
                None | Some(Tk::Close) => break,
                _ => {}
            }
            if self.word_is("AND") {
                self.i += 1;
                if let Some(last) = items.last_mut()
                    && last.0 == Occ::Should
                {
                    last.0 = Occ::Must;
                }
                and_next = true;
                continue;
            }
            if self.word_is("OR") {
                self.i += 1;
                continue;
            }
            let mut occ = Occ::Should;
            if self.word_is("NOT") {
                self.i += 1;
                occ = Occ::Not;
            }
            match self.peek() {
                Some(Tk::Plus) => {
                    self.i += 1;
                    occ = Occ::Must;
                }
                Some(Tk::Minus) => {
                    self.i += 1;
                    occ = Occ::Not;
                }
                _ => {}
            }
            if and_next && occ == Occ::Should {
                occ = Occ::Must;
            }
            and_next = false;
            let q = self.clause()?;
            items.push((occ, q));
        }
        Ok(bool_of(items))
    }

    fn fields_for(&mut self) -> Vec<String> {
        if let (Some(Tk::Word(w)), Some(Tk::Colon)) = (self.t.get(self.i), self.t.get(self.i + 1)) {
            let f = w.clone();
            self.i += 2;
            return vec![f];
        }
        self.fields.clone()
    }

    fn clause(&mut self) -> Result<SearchQuery> {
        let fields = self.fields_for();
        let q = match self.peek().cloned() {
            Some(Tk::Open) => {
                self.i += 1;
                let saved = std::mem::replace(&mut self.fields, fields);
                let q = self.clauses()?;
                self.fields = saved;
                if self.peek() != Some(&Tk::Close) {
                    return Err(syntax("missing ')' in search query"));
                }
                self.i += 1;
                q
            }
            Some(Tk::Quoted(p)) => {
                self.i += 1;
                let mut slop = 0;
                if self.peek() == Some(&Tk::Tilde) {
                    self.i += 1;
                    if let Some(Tk::Word(n)) = self.peek().cloned() {
                        slop = n.parse().map_err(|_| syntax(format!("bad slop '{n}'")))?;
                        self.i += 1;
                    }
                }
                SearchQuery::Phrase {
                    fields,
                    text: p,
                    slop,
                }
            }
            Some(Tk::Word(w)) => {
                self.i += 1;
                match self.peek() {
                    Some(Tk::Star) => {
                        self.i += 1;
                        SearchQuery::Prefix { fields, text: w }
                    }
                    Some(Tk::Tilde) => {
                        self.i += 1;
                        let mut d = 2;
                        if let Some(Tk::Word(n)) = self.peek().cloned()
                            && let Ok(x) = n.parse::<u8>()
                        {
                            d = x;
                            self.i += 1;
                        }
                        if d > 2 {
                            return Err(syntax("fuzzy distance must be 1 or 2"));
                        }
                        SearchQuery::Match {
                            fields,
                            text: w,
                            all: false,
                            fuzzy: Some(d),
                        }
                    }
                    _ => SearchQuery::Match {
                        fields,
                        text: w,
                        all: false,
                        fuzzy: None,
                    },
                }
            }
            other => return Err(syntax(format!("unexpected {other:?} in search query"))),
        };
        if self.peek() == Some(&Tk::Caret) {
            self.i += 1;
            let Some(Tk::Word(n)) = self.peek().cloned() else {
                return Err(syntax("'^' needs a number"));
            };
            self.i += 1;
            let b: f32 = n.parse().map_err(|_| syntax(format!("bad boost '{n}'")))?;
            return Ok(SearchQuery::Boost(Box::new(q), b));
        }
        Ok(q)
    }
}

fn bool_of(items: Vec<(Occ, SearchQuery)>) -> SearchQuery {
    let (mut must, mut should, mut must_not) = (Vec::new(), Vec::new(), Vec::new());
    for (o, q) in items {
        match o {
            Occ::Must => must.push(q),
            Occ::Should => should.push(q),
            Occ::Not => must_not.push(q),
        }
    }
    if must.is_empty() && must_not.is_empty() && should.len() == 1 {
        return should.pop().unwrap_or(SearchQuery::All);
    }
    SearchQuery::Bool {
        must,
        should,
        must_not,
        filter: Vec::new(),
        min_should: 0,
    }
}

/// A `sprig_search()` / `queryString` query over `default_fields` (empty:
/// the index's default text fields).
///
/// # Errors
/// 39822 for malformed text.
pub fn parse_query_string(s: &str, default_fields: &[String]) -> Result<SearchQuery> {
    let t = lex(s, false)?;
    let mut p = Qs {
        t: &t,
        i: 0,
        fields: default_fields.to_vec(),
    };
    let q = p.clauses()?;
    if p.i < t.len() {
        return Err(syntax("unbalanced ')' in search query"));
    }
    Ok(q)
}

/// `MATCH (columns) AGAINST ('...' IN BOOLEAN MODE)`.
///
/// # Errors
/// 39822 for malformed text.
pub fn parse_mysql_boolean(s: &str, columns: &[String]) -> Result<SearchQuery> {
    let t = lex(s, true)?;
    let mut i = 0;
    let q = mysql_group(&t, &mut i, columns)?;
    Ok(q)
}

fn mysql_group(t: &[Tk], i: &mut usize, cols: &[String]) -> Result<SearchQuery> {
    let mut items: Vec<(Occ, SearchQuery)> = Vec::new();
    while *i < t.len() {
        let mut occ = Occ::Should;
        let mut boost: Option<f32> = None;
        loop {
            match t.get(*i) {
                Some(Tk::Plus) => occ = Occ::Must,
                Some(Tk::Minus) => occ = Occ::Not,
                Some(Tk::Gt) => boost = Some(1.5),
                Some(Tk::Lt) => boost = Some(0.67),
                Some(Tk::Tilde) => boost = Some(0.5),
                _ => break,
            }
            *i += 1;
        }
        let q = match t.get(*i).cloned() {
            None => break,
            Some(Tk::Close) => {
                *i += 1;
                return Ok(bool_of(items));
            }
            Some(Tk::Open) => {
                *i += 1;
                mysql_group(t, i, cols)?
            }
            Some(Tk::Quoted(p)) => {
                *i += 1;
                SearchQuery::Phrase {
                    fields: cols.to_vec(),
                    text: p,
                    slop: 0,
                }
            }
            Some(Tk::Word(w)) => {
                *i += 1;
                if t.get(*i) == Some(&Tk::Star) {
                    *i += 1;
                    SearchQuery::Prefix {
                        fields: cols.to_vec(),
                        text: w,
                    }
                } else {
                    SearchQuery::Match {
                        fields: cols.to_vec(),
                        text: w,
                        all: false,
                        fuzzy: None,
                    }
                }
            }
            Some(other) => {
                // A stray operator (`*` alone, `@distance`): MariaDB ignores it.
                let _ = other;
                *i += 1;
                continue;
            }
        };
        let q = match boost {
            Some(b) => SearchQuery::Boost(Box::new(q), b),
            None => q,
        };
        items.push((occ, q));
    }
    Ok(bool_of(items))
}

/// `MATCH (columns) AGAINST ('...')` in natural language mode.
#[must_use]
pub fn mysql_natural(s: &str, columns: &[String]) -> SearchQuery {
    SearchQuery::Match {
        fields: columns.to_vec(),
        text: s.to_owned(),
        all: false,
        fuzzy: None,
    }
}

/// `$text: {$search: s}` over every text field of the index.
#[must_use]
pub fn parse_mongo_text(s: &str) -> SearchQuery {
    let mut must = Vec::new();
    let mut should = Vec::new();
    let mut must_not = Vec::new();
    let mut chars = s.chars().peekable();
    let mut word = String::new();
    let mut negated = false;
    let flush = |w: &mut String, neg: &mut bool, should: &mut Vec<_>, not: &mut Vec<_>| {
        if !w.is_empty() {
            let q = SearchQuery::Match {
                fields: Vec::new(),
                text: std::mem::take(w),
                all: false,
                fuzzy: None,
            };
            if *neg { not.push(q) } else { should.push(q) }
        }
        *neg = false;
    };
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                let neg_phrase = negated && word.is_empty();
                flush(&mut word, &mut negated, &mut should, &mut must_not);
                let mut p = String::new();
                for c in chars.by_ref() {
                    if c == '"' {
                        break;
                    }
                    p.push(c);
                }
                let q = SearchQuery::Phrase {
                    fields: Vec::new(),
                    text: p,
                    slop: 0,
                };
                if neg_phrase {
                    must_not.push(q);
                } else {
                    must.push(q);
                }
            }
            '-' if word.is_empty() => negated = true,
            c if c.is_whitespace() => flush(&mut word, &mut negated, &mut should, &mut must_not),
            c => word.push(c),
        }
    }
    flush(&mut word, &mut negated, &mut should, &mut must_not);
    // Phrases are required, terms score; with no phrase a term must match.
    if must.is_empty() && should.is_empty() {
        // Only negations: MongoDB matches nothing.
        return SearchQuery::Bool {
            must: Vec::new(),
            should: Vec::new(),
            must_not: Vec::new(),
            filter: Vec::new(),
            min_should: 1,
        };
    }
    let min_should = usize::from(must.is_empty());
    SearchQuery::Bool {
        must,
        should,
        must_not,
        filter: Vec::new(),
        min_should,
    }
}

// ---- Atlas `$search` ----

/// Paths of an Atlas operator: a string, an array, or `{wildcard: "*"}`
/// (every text field: empty).
fn paths(op: &Json) -> Result<Vec<String>> {
    match op.get("path") {
        None => Err(syntax("search operator needs 'path'")),
        Some(Json::String(s)) if s == "*" => Ok(Vec::new()),
        Some(Json::String(s)) => Ok(vec![s.clone()]),
        Some(Json::Array(a)) => {
            let mut v = Vec::new();
            for x in a {
                match x {
                    Json::String(s) => v.push(s.clone()),
                    Json::Object(o) if o.get("wildcard").and_then(Json::as_str) == Some("*") => {
                        return Ok(Vec::new());
                    }
                    Json::Object(o) if o.contains_key("value") => {
                        v.push(o["value"].as_str().unwrap_or_default().to_owned());
                    }
                    _ => return Err(syntax("bad 'path' entry")),
                }
            }
            Ok(v)
        }
        Some(Json::Object(o)) if o.get("wildcard").and_then(Json::as_str) == Some("*") => {
            Ok(Vec::new())
        }
        Some(Json::Object(o)) if o.get("wildcard").is_some() => Err(unsupported(
            "path wildcards other than '*' are not supported",
        )),
        Some(_) => Err(syntax("bad 'path'")),
    }
}

fn queries(op: &Json) -> Result<Vec<String>> {
    match op.get("query") {
        Some(Json::String(s)) => Ok(vec![s.clone()]),
        Some(Json::Array(a)) => a
            .iter()
            .map(|x| {
                x.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| syntax("'query' entries must be strings"))
            })
            .collect(),
        _ => Err(syntax("search operator needs 'query'")),
    }
}

fn boosted(op: &Json, q: SearchQuery) -> Result<SearchQuery> {
    match op.get("score") {
        None => Ok(q),
        Some(s) => {
            if let Some(b) = s.get("boost") {
                let v = b.get("value").and_then(Json::as_f64).ok_or_else(|| {
                    unsupported("score.boost needs a 'value' (path boosts are not supported)")
                })?;
                #[allow(clippy::cast_possible_truncation)]
                return Ok(SearchQuery::Boost(Box::new(q), v as f32));
            }
            Err(unsupported("only score.boost is supported"))
        }
    }
}

fn lit(v: &Json) -> Result<Lit> {
    Ok(match v {
        Json::Number(n) => Lit::Num(n.as_f64().unwrap_or(0.0)),
        Json::String(s) => Lit::Str(s.clone()),
        Json::Bool(b) => Lit::Bool(*b),
        Json::Object(o) if o.contains_key("$date") => match &o["$date"] {
            Json::String(s) => Lit::Str(s.clone()),
            Json::Number(n) => Lit::DateMs(n.as_i64().unwrap_or(0)),
            Json::Object(x) => Lit::DateMs(
                x.get("$numberLong")
                    .and_then(Json::as_str)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
            ),
            _ => return Err(syntax("bad $date")),
        },
        Json::Object(o) if o.contains_key("$oid") => {
            Lit::Str(o["$oid"].as_str().unwrap_or_default().to_owned())
        }
        Json::Object(o) if o.contains_key("$numberLong") || o.contains_key("$numberDecimal") => {
            let s = o.values().next().and_then(Json::as_str).unwrap_or_default();
            Lit::Num(s.parse().map_err(|_| syntax("bad number"))?)
        }
        _ => return Err(syntax(format!("unsupported value {v}"))),
    })
}

/// One `$search` operator (the stage's document without `index`,
/// `highlight`, `count`, ...: exactly one operator key).
///
/// # Errors
/// 39822 for malformed operators, 39823 for operators not offered.
pub fn parse_atlas(stage: &Json) -> Result<SearchQuery> {
    const NOT_OPS: [&str; 10] = [
        "consistency",
        "index",
        "highlight",
        "count",
        "returnStoredSource",
        "scoreDetails",
        "sort",
        "concurrent",
        "tracking",
        "searchBefore",
    ];
    let obj = stage
        .as_object()
        .ok_or_else(|| syntax("$search needs an object"))?;
    let mut ops = obj
        .iter()
        .filter(|(k, _)| !NOT_OPS.contains(&k.as_str()) && k.as_str() != "searchAfter");
    let Some((name, op)) = ops.next() else {
        return Err(syntax("$search needs an operator"));
    };
    if ops.next().is_some() {
        return Err(syntax("$search takes exactly one operator"));
    }
    operator(name, op)
}

fn operator(name: &str, op: &Json) -> Result<SearchQuery> {
    let q = match name {
        "text" => {
            let fields = paths(op)?;
            let fuzzy = op.get("fuzzy").map(|f| {
                u8::try_from(f.get("maxEdits").and_then(Json::as_u64).unwrap_or(2))
                    .unwrap_or(2)
                    .min(2)
            });
            if op.get("synonyms").is_some() {
                return Err(unsupported(
                    "text.synonyms is not supported: give the index a custom analyzer with synonyms",
                ));
            }
            let all = op.get("matchCriteria").and_then(Json::as_str) == Some("all");
            let qs = queries(op)?;
            let parts: Vec<SearchQuery> = qs
                .into_iter()
                .map(|t| SearchQuery::Match {
                    fields: fields.clone(),
                    text: t,
                    all,
                    fuzzy,
                })
                .collect();
            one_or_any(parts)
        }
        "phrase" => {
            let fields = paths(op)?;
            let slop =
                u32::try_from(op.get("slop").and_then(Json::as_u64).unwrap_or(0)).unwrap_or(0);
            one_or_any(
                queries(op)?
                    .into_iter()
                    .map(|t| SearchQuery::Phrase {
                        fields: fields.clone(),
                        text: t,
                        slop,
                    })
                    .collect(),
            )
        }
        "autocomplete" => {
            let fields = paths(op)?;
            let fuzzy = op.get("fuzzy").map(|f| {
                u8::try_from(f.get("maxEdits").and_then(Json::as_u64).unwrap_or(2))
                    .unwrap_or(2)
                    .min(2)
            });
            let all = op.get("tokenOrder").and_then(Json::as_str) == Some("sequential");
            one_or_any(
                queries(op)?
                    .into_iter()
                    .map(|t| SearchQuery::Match {
                        fields: fields.clone(),
                        text: t,
                        all,
                        fuzzy,
                    })
                    .collect(),
            )
        }
        "compound" => {
            let list = |k: &str| -> Result<Vec<SearchQuery>> {
                match op.get(k) {
                    None => Ok(Vec::new()),
                    Some(Json::Array(a)) => a.iter().map(clause).collect(),
                    Some(o @ Json::Object(_)) => Ok(vec![clause(o)?]),
                    Some(_) => Err(syntax(format!("compound.{k} must be an array"))),
                }
            };
            let must = list("must")?;
            let should = list("should")?;
            let must_not = list("mustNot")?;
            let filter = list("filter")?;
            let min_should = usize::try_from(
                op.get("minimumShouldMatch")
                    .and_then(Json::as_u64)
                    .unwrap_or(0),
            )
            .unwrap_or(0);
            if must.is_empty() && should.is_empty() && filter.is_empty() && must_not.is_empty() {
                return Err(syntax("compound needs at least one clause"));
            }
            SearchQuery::Bool {
                must,
                should,
                must_not,
                filter,
                min_should,
            }
        }
        "range" => {
            let field = single_path(op)?;
            let bound = |k_ex: &str, k_in: &str| -> Result<Bound<Lit>> {
                Ok(match (op.get(k_ex), op.get(k_in)) {
                    (Some(v), _) => Bound::Excluded(lit(v)?),
                    (None, Some(v)) => Bound::Included(lit(v)?),
                    (None, None) => Bound::Unbounded,
                })
            };
            SearchQuery::Range {
                field,
                lo: bound("gt", "gte")?,
                hi: bound("lt", "lte")?,
            }
        }
        "equals" => SearchQuery::Equals {
            field: single_path(op)?,
            value: lit(op
                .get("value")
                .ok_or_else(|| syntax("equals needs 'value'"))?)?,
        },
        "in" => {
            let field = single_path(op)?;
            let vals = match op.get("value") {
                Some(Json::Array(a)) => a.iter().map(lit).collect::<Result<Vec<_>>>()?,
                Some(v) => vec![lit(v)?],
                None => return Err(syntax("in needs 'value'")),
            };
            SearchQuery::any(
                vals.into_iter()
                    .map(|value| SearchQuery::Equals {
                        field: field.clone(),
                        value,
                    })
                    .collect(),
            )
        }
        "exists" => SearchQuery::Exists(single_path(op)?),
        "wildcard" | "regex" => {
            let fields = paths(op)?;
            let qs = queries(op)?;
            one_or_any(
                qs.into_iter()
                    .map(|p| {
                        if name == "wildcard" {
                            SearchQuery::Wildcard {
                                fields: fields.clone(),
                                pattern: p,
                            }
                        } else {
                            SearchQuery::Regex {
                                fields: fields.clone(),
                                pattern: p,
                            }
                        }
                    })
                    .collect(),
            )
        }
        "queryString" => {
            let dp = op
                .get("defaultPath")
                .and_then(Json::as_str)
                .ok_or_else(|| syntax("queryString needs 'defaultPath'"))?;
            let q = op
                .get("query")
                .and_then(Json::as_str)
                .ok_or_else(|| syntax("queryString needs 'query'"))?;
            parse_query_string(q, &[dp.to_owned()])?
        }
        "embeddedDocument" | "geoShape" | "geoWithin" | "near" | "moreLikeThis" | "knnBeta"
        | "span" => {
            return Err(unsupported(format!(
                "the $search operator '{name}' is not supported"
            )));
        }
        other => return Err(syntax(format!("unknown $search operator '{other}'"))),
    };
    boosted(op, q)
}

fn clause(c: &Json) -> Result<SearchQuery> {
    let o = c
        .as_object()
        .ok_or_else(|| syntax("a compound clause must be an object"))?;
    let mut it = o.iter();
    let (Some((n, op)), None) = (it.next(), it.next()) else {
        return Err(syntax("a compound clause takes exactly one operator"));
    };
    operator(n, op)
}

fn single_path(op: &Json) -> Result<String> {
    let mut p = paths(op)?;
    if p.len() != 1 {
        return Err(syntax("this operator takes one 'path'"));
    }
    Ok(p.remove(0))
}

fn one_or_any(mut v: Vec<SearchQuery>) -> SearchQuery {
    if v.len() == 1 {
        v.remove(0)
    } else {
        SearchQuery::any(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn m(t: &str) -> SearchQuery {
        SearchQuery::Match {
            fields: Vec::new(),
            text: t.into(),
            all: false,
            fuzzy: None,
        }
    }

    #[test]
    fn query_string_operators() {
        let q =
            parse_query_string(r#"red "running shoe"~1 -kids name:trail~1 sho* x^2"#, &[]).unwrap();
        let SearchQuery::Bool {
            must,
            should,
            must_not,
            ..
        } = q
        else {
            panic!("{q:?}")
        };
        assert!(must.is_empty());
        assert_eq!(must_not, vec![m("kids")]);
        assert_eq!(should[0], m("red"));
        assert_eq!(
            should[1],
            SearchQuery::Phrase {
                fields: vec![],
                text: "running shoe".into(),
                slop: 1
            }
        );
        assert_eq!(
            should[2],
            SearchQuery::Match {
                fields: vec!["name".into()],
                text: "trail".into(),
                all: false,
                fuzzy: Some(1)
            }
        );
        assert_eq!(
            should[3],
            SearchQuery::Prefix {
                fields: vec![],
                text: "sho".into()
            }
        );
        assert_eq!(should[4], SearchQuery::Boost(Box::new(m("x")), 2.0));
    }

    #[test]
    fn query_string_and_or_groups() {
        let q = parse_query_string("a AND (b OR c)", &[]).unwrap();
        let SearchQuery::Bool { must, should, .. } = q else {
            panic!()
        };
        assert_eq!(must.len(), 2);
        assert!(should.is_empty());
        assert!(parse_query_string("(a b", &[]).is_err());
        assert!(parse_query_string("a)", &[]).is_err());
    }

    #[test]
    fn mysql_boolean_mode() {
        let cols = vec!["title".to_owned()];
        let q = parse_mysql_boolean("+apple -banana >pie (cake tart*)", &cols).unwrap();
        let SearchQuery::Bool {
            must,
            should,
            must_not,
            ..
        } = q
        else {
            panic!()
        };
        assert_eq!(must.len(), 1);
        assert_eq!(must_not.len(), 1);
        assert_eq!(should.len(), 2);
        assert!(matches!(should[0], SearchQuery::Boost(_, b) if (b - 1.5).abs() < 1e-6));
    }

    #[test]
    fn mongo_text_string() {
        let q = parse_mongo_text(r#"coffee -shop "iced latte""#);
        let SearchQuery::Bool {
            must,
            should,
            must_not,
            min_should,
            ..
        } = q
        else {
            panic!()
        };
        assert_eq!(should, vec![m("coffee")]);
        assert_eq!(must_not, vec![m("shop")]);
        assert_eq!(must.len(), 1);
        assert_eq!(min_should, 0);
        let only_neg = parse_mongo_text("-a");
        assert!(
            matches!(only_neg, SearchQuery::Bool { min_should: 1, ref should, .. } if should.is_empty())
        );
    }

    #[test]
    fn atlas_operators() {
        let q = parse_atlas(&json!({"index": "default", "compound": {
            "must": [{"text": {"query": "shoe", "path": "name", "fuzzy": {"maxEdits": 1}}}],
            "filter": [{"range": {"path": "price", "gte": 10, "lt": 100}}],
            "mustNot": {"equals": {"path": "brand", "value": "acme"}},
            "should": [{"phrase": {"query": "red shoe", "path": ["name", "desc"], "slop": 2,
                         "score": {"boost": {"value": 3}}}}]
        }}))
        .unwrap();
        let SearchQuery::Bool {
            must,
            filter,
            must_not,
            should,
            ..
        } = q
        else {
            panic!()
        };
        assert_eq!(must.len(), 1);
        assert!(matches!(
            &filter[0],
            SearchQuery::Range {
                lo: Bound::Included(Lit::Num(_)),
                hi: Bound::Excluded(_),
                ..
            }
        ));
        assert!(matches!(&must_not[0], SearchQuery::Equals { .. }));
        assert!(matches!(&should[0], SearchQuery::Boost(_, b) if (*b - 3.0).abs() < 1e-6));
        assert_eq!(
            parse_atlas(&json!({"knnBeta": {}})).unwrap_err().code(),
            Some(SprigCode::SearchUnsupported)
        );
        assert_eq!(
            parse_atlas(&json!({"bogus": {}})).unwrap_err().code(),
            Some(SprigCode::SearchQuerySyntax)
        );
    }
}
