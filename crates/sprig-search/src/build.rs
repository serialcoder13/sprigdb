//! Tantivy queries from the AST, scored with global BM25.
//!
//! Text is analyzed with each field's query analyzer; synonyms become an
//! OR of terms. Fuzzy and prefix terms are **expanded once**, over every
//! partition ([`Expander`]), into an OR of concrete terms scored with
//! BM25, so every partition expands them the same way and scores agree
//! (Tantivy's own fuzzy query scores with a constant). Wildcards and
//! regular expressions keep a constant score, as in Atlas.

use std::ops::Bound;

use sprig_common::{Result, SprigCode, SprigError};
use tantivy::Term;
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, EmptyQuery, ExistsQuery, Occur, PhraseQuery, Query,
    RangeQuery, RegexQuery, TermQuery, TermSetQuery,
};
use tantivy::schema::{Field, IndexRecordOption};

use crate::analyze::Analyzer;
use crate::def::{FieldDef, FieldKind};
use crate::query::{Lit, SearchQuery};
use crate::schema::{IndexSchema, date_of};

/// Where terms of a field live: a plain field, or a path of `_dyn`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TermBase {
    /// The Tantivy field.
    pub field: Field,
    /// The JSON path in `_dyn`.
    pub json_path: Option<String>,
}

impl TermBase {
    /// The term of `text` here.
    #[must_use]
    pub fn term(&self, text: &str) -> Term {
        match &self.json_path {
            None => Term::from_field_text(self.field, text),
            Some(p) => {
                let mut t = Term::from_field_json_path(self.field, p, false);
                t.append_type_and_str(text);
                t
            }
        }
    }

    /// The bytes of a term's value before its text (the JSON path and
    /// type of a `_dyn` term; nothing for a plain field).
    #[must_use]
    pub fn value_prefix(&self) -> Vec<u8> {
        self.term("").serialized_value_bytes().to_vec()
    }
}

/// How a term is expanded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expansion {
    /// Terms starting with this.
    Prefix(String),
    /// Terms within this edit distance.
    Fuzzy(String, u8),
}

/// Expands terms over every partition of the index.
pub trait Expander {
    /// Concrete terms (their text) for `how` on `base`, at most `max`.
    ///
    /// # Errors
    /// Tantivy's.
    fn expand(&self, base: &TermBase, how: &Expansion, max: usize) -> Result<Vec<String>>;
}

/// An expander that expands nothing (tests, and queries without fuzzy or
/// prefix terms).
#[derive(Debug, Clone, Copy)]
pub struct NoExpansion;

impl Expander for NoExpansion {
    fn expand(&self, _: &TermBase, how: &Expansion, _: usize) -> Result<Vec<String>> {
        Ok(match how {
            Expansion::Prefix(p) | Expansion::Fuzzy(p, _) => vec![p.clone()],
        })
    }
}

fn syntax(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, msg)
}

/// A field a query reaches: where its terms are, its analyzer, its kind
/// and boost.
struct Target<'a> {
    base: TermBase,
    analyzer: &'a Analyzer,
    kind: FieldKind,
    boost: f32,
    fast_name: Option<String>,
}

/// Builds Tantivy queries for one index.
pub struct Builder<'a> {
    schema: &'a IndexSchema,
    expander: &'a dyn Expander,
    max_expansions: usize,
}

impl std::fmt::Debug for Builder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder").finish_non_exhaustive()
    }
}

fn boxed(q: impl Query) -> Box<dyn Query> {
    Box::new(q)
}

fn or(mut qs: Vec<Box<dyn Query>>) -> Box<dyn Query> {
    match qs.len() {
        0 => boxed(EmptyQuery),
        1 => qs.remove(0),
        _ => boxed(BooleanQuery::new(
            qs.into_iter().map(|q| (Occur::Should, q)).collect(),
        )),
    }
}

fn and(mut qs: Vec<Box<dyn Query>>) -> Box<dyn Query> {
    match qs.len() {
        0 => boxed(EmptyQuery),
        1 => qs.remove(0),
        _ => boxed(BooleanQuery::new(
            qs.into_iter().map(|q| (Occur::Must, q)).collect(),
        )),
    }
}

fn boost(q: Box<dyn Query>, b: f32) -> Box<dyn Query> {
    if (b - 1.0).abs() < f32::EPSILON {
        q
    } else {
        boxed(BoostQuery::new(q, b))
    }
}

/// `*` and `?` as a regular expression.
fn wildcard_regex(p: &str) -> String {
    let mut out = String::new();
    for c in p.chars() {
        match c {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            c if "\\.+()|[]{}^$#&-~".contains(c) => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out
}

impl<'a> Builder<'a> {
    /// A builder over `schema`.
    #[must_use]
    pub fn new(schema: &'a IndexSchema, expander: &'a dyn Expander, max_expansions: usize) -> Self {
        Self {
            schema,
            expander,
            max_expansions,
        }
    }

    fn target(&self, f: &'a FieldDef) -> Result<Target<'a>> {
        let def = &self.schema.def;
        let an = f
            .query_analyzer
            .as_deref()
            .unwrap_or_else(|| f.index_analyzer(&def.default_analyzer));
        // Autocomplete and substring fields are queried with the plain chain.
        let an = match f.kind {
            FieldKind::Substring => "trigram",
            _ => an,
        };
        #[allow(clippy::cast_possible_truncation)]
        Ok(Target {
            base: TermBase {
                field: self.schema.field(f)?,
                json_path: None,
            },
            analyzer: self.schema.analyzers.get(an)?,
            kind: f.kind,
            boost: f.boost as f32,
            fast_name: f.fast().then(|| format!("f{}", f.field_no)),
        })
    }

    /// Targets of `names` for text operators (empty: the default fields).
    fn text_targets(&self, names: &[String]) -> Result<Vec<Target<'a>>> {
        let def = &self.schema.def;
        let mut out = Vec::new();
        if names.is_empty() {
            for f in def.default_fields() {
                out.push(self.target(f)?);
            }
            if let Some(all) = self.schema.all {
                out.push(Target {
                    base: TermBase {
                        field: all,
                        json_path: None,
                    },
                    analyzer: self.schema.analyzers.get(&def.default_analyzer)?,
                    kind: FieldKind::Text,
                    boost: 1.0,
                    fast_name: None,
                });
            }
            return Ok(out);
        }
        for n in names {
            let found: Vec<&FieldDef> = def
                .fields
                .iter()
                .filter(|f| f.name.eq_ignore_ascii_case(n))
                .collect();
            // A path with several kinds: text kinds first.
            let mut text: Vec<&&FieldDef> = found.iter().filter(|f| f.kind.is_text()).collect();
            if text.is_empty() {
                text = found.iter().collect();
            }
            if text.is_empty() {
                if let Some(d) = self.schema.dynamic {
                    out.push(Target {
                        base: TermBase {
                            field: d,
                            json_path: Some(n.clone()),
                        },
                        analyzer: self.schema.analyzers.get(&def.default_analyzer)?,
                        kind: FieldKind::Text,
                        boost: 1.0,
                        fast_name: None,
                    });
                }
                continue;
            }
            for f in text {
                out.push(self.target(f)?);
            }
        }
        Ok(out)
    }

    fn value_target(&self, name: &str, want: &[FieldKind]) -> Result<Option<Target<'a>>> {
        let def = &self.schema.def;
        let f = def
            .fields
            .iter()
            .filter(|f| f.name.eq_ignore_ascii_case(name))
            .find(|f| want.contains(&f.kind));
        f.map(|f| self.target(f)).transpose()
    }

    fn term_alternatives(
        &self,
        t: &Target<'_>,
        token: &str,
        fuzzy: Option<u8>,
    ) -> Result<Vec<String>> {
        let mut alts = t.analyzer.expand(token);
        if let Some(d) = fuzzy.filter(|d| *d > 0) {
            let mut out = Vec::new();
            for a in &alts {
                out.extend(self.expander.expand(
                    &t.base,
                    &Expansion::Fuzzy(a.clone(), d),
                    self.max_expansions,
                )?);
            }
            out.sort();
            out.dedup();
            alts = out;
        }
        Ok(alts)
    }

    fn term_query(t: &Target<'_>, text: &str) -> Box<dyn Query> {
        boxed(TermQuery::new(
            t.base.term(text),
            IndexRecordOption::WithFreqs,
        ))
    }

    fn matcher(
        &self,
        fields: &[String],
        text: &str,
        all: bool,
        fuzzy: Option<u8>,
    ) -> Result<Box<dyn Query>> {
        let targets = self.text_targets(fields)?;
        // Per target, its tokens' alternatives.
        let mut per_token: Vec<Vec<Box<dyn Query>>> = Vec::new();
        let mut flat: Vec<Box<dyn Query>> = Vec::new();
        for t in &targets {
            let toks = t.analyzer.query_tokens(text);
            for (i, tok) in toks.iter().enumerate() {
                let alts = self.term_alternatives(t, &tok.text, fuzzy)?;
                let q = boost(
                    or(alts.iter().map(|a| Self::term_query(t, a)).collect()),
                    t.boost,
                );
                if all {
                    if per_token.len() <= i {
                        per_token.resize_with(i + 1, Vec::new);
                    }
                    per_token[i].push(q);
                } else {
                    flat.push(q);
                }
            }
        }
        Ok(if all {
            and(per_token.into_iter().map(or).collect())
        } else {
            or(flat)
        })
    }

    fn phrase(&self, fields: &[String], text: &str, slop: u32) -> Result<Box<dyn Query>> {
        let mut out = Vec::new();
        for t in self.text_targets(fields)? {
            let toks = t.analyzer.query_tokens(text);
            let q = match toks.len() {
                0 => continue,
                1 => Self::term_query(&t, &toks[0].text),
                _ => {
                    let first = toks[0].position;
                    let terms: Vec<(usize, Term)> = toks
                        .iter()
                        .map(|k| (k.position - first, t.base.term(&k.text)))
                        .collect();
                    boxed(PhraseQuery::new_with_offset_and_slop(terms, slop))
                }
            };
            out.push(boost(q, t.boost));
        }
        Ok(or(out))
    }

    fn prefix(&self, fields: &[String], text: &str) -> Result<Box<dyn Query>> {
        let mut out = Vec::new();
        for t in self.text_targets(fields)? {
            // The prefix as the field's analyzer sees words: lowercase.
            let p = match t.kind {
                FieldKind::Keyword => text.to_owned(),
                _ => text.to_lowercase(),
            };
            let terms =
                self.expander
                    .expand(&t.base, &Expansion::Prefix(p), self.max_expansions)?;
            out.push(boost(
                or(terms.iter().map(|x| Self::term_query(&t, x)).collect()),
                t.boost,
            ));
        }
        Ok(or(out))
    }

    fn regex(&self, fields: &[String], pattern: &str) -> Result<Box<dyn Query>> {
        let mut out = Vec::new();
        for t in self.text_targets(fields)? {
            if t.base.json_path.is_some() {
                continue;
            }
            let q = RegexQuery::from_pattern(pattern, t.base.field)
                .map_err(|e| syntax(format!("bad regular expression: {e}")))?;
            out.push(boxed(q));
        }
        Ok(or(out))
    }

    fn lit_term(t: &Target<'_>, v: &Lit) -> Result<Term> {
        let f = t.base.field;
        Ok(match (t.kind, v) {
            (FieldKind::Number, Lit::Num(n)) => Term::from_field_f64(f, *n),
            (FieldKind::Bool, Lit::Bool(b)) => Term::from_field_bool(f, *b),
            (FieldKind::Date, Lit::DateMs(ms)) => {
                Term::from_field_date_for_search(f, tantivy::DateTime::from_timestamp_millis(*ms))
            }
            (FieldKind::Date, Lit::Str(s)) => Term::from_field_date_for_search(
                f,
                date_of(s).ok_or_else(|| syntax(format!("bad date '{s}'")))?,
            ),
            (FieldKind::Keyword | FieldKind::Text, Lit::Str(s)) => Term::from_field_text(f, s),
            (FieldKind::Keyword, Lit::Num(n)) => Term::from_field_text(f, &n.to_string()),
            (FieldKind::Keyword, Lit::Bool(b)) => Term::from_field_text(f, &b.to_string()),
            (k, v) => {
                return Err(SprigError::new(
                    SprigCode::SearchFieldType,
                    format!("a {} field can't be compared with {v:?}", k.as_str()),
                ));
            }
        })
    }

    /// The Tantivy query of `q`.
    ///
    /// # Errors
    /// 39822 for malformed parts, 39829 for a value of the wrong type,
    /// the expander's errors.
    pub fn build(&self, q: &SearchQuery) -> Result<Box<dyn Query>> {
        Ok(match q {
            SearchQuery::All => boxed(AllQuery),
            SearchQuery::Match {
                fields,
                text,
                all,
                fuzzy,
            } => self.matcher(fields, text, *all, *fuzzy)?,
            SearchQuery::Phrase { fields, text, slop } => self.phrase(fields, text, *slop)?,
            SearchQuery::Prefix { fields, text } => self.prefix(fields, text)?,
            SearchQuery::Wildcard { fields, pattern } => {
                self.regex(fields, &wildcard_regex(pattern))?
            }
            SearchQuery::Regex { fields, pattern } => self.regex(fields, pattern)?,
            SearchQuery::Exists(name) => {
                let def = &self.schema.def;
                let Some(f) = def
                    .fields
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(name))
                else {
                    return Ok(boxed(EmptyQuery));
                };
                let t = self.target(f)?;
                match t.fast_name {
                    Some(n) => boxed(ExistsQuery::new(n, false)),
                    None => boxed(
                        RegexQuery::from_pattern(".*", t.base.field)
                            .map_err(|e| syntax(e.to_string()))?,
                    ),
                }
            }
            SearchQuery::Range { field, lo, hi } => {
                let Some(t) = self.value_target(
                    field,
                    &[FieldKind::Number, FieldKind::Date, FieldKind::Keyword],
                )?
                else {
                    return Ok(boxed(EmptyQuery));
                };
                let b = |x: &Bound<Lit>| -> Result<Bound<Term>> {
                    Ok(match x {
                        Bound::Included(v) => Bound::Included(Self::lit_term(&t, v)?),
                        Bound::Excluded(v) => Bound::Excluded(Self::lit_term(&t, v)?),
                        Bound::Unbounded => Bound::Unbounded,
                    })
                };
                let (lo, hi) = (b(lo)?, b(hi)?);
                if matches!((&lo, &hi), (Bound::Unbounded, Bound::Unbounded)) {
                    return Err(syntax("range needs at least one bound"));
                }
                boxed(RangeQuery::new(lo, hi))
            }
            SearchQuery::Equals { field, value } => {
                let kinds: &[FieldKind] = match value {
                    Lit::Num(_) => &[FieldKind::Number, FieldKind::Keyword],
                    Lit::Bool(_) => &[FieldKind::Bool, FieldKind::Keyword],
                    Lit::DateMs(_) => &[FieldKind::Date],
                    Lit::Str(_) => &[FieldKind::Keyword, FieldKind::Date, FieldKind::Text],
                };
                let Some(t) = self.value_target(field, kinds)? else {
                    return Ok(boxed(EmptyQuery));
                };
                boxed(TermQuery::new(
                    Self::lit_term(&t, value)?,
                    IndexRecordOption::Basic,
                ))
            }
            SearchQuery::Bool {
                must,
                should,
                must_not,
                filter,
                min_should,
            } => {
                let positive = !must.is_empty() || !should.is_empty() || !filter.is_empty();
                if !positive || (*min_should > should.len()) {
                    return Ok(boxed(EmptyQuery));
                }
                let mut v: Vec<(Occur, Box<dyn Query>)> = Vec::new();
                for x in must {
                    v.push((Occur::Must, self.build(x)?));
                }
                for x in filter {
                    v.push((Occur::Must, boxed(BoostQuery::new(self.build(x)?, 0.0))));
                }
                for x in should {
                    v.push((Occur::Should, self.build(x)?));
                }
                for x in must_not {
                    v.push((Occur::MustNot, self.build(x)?));
                }
                if *min_should > 1 || (*min_should == 1 && !(must.is_empty() && filter.is_empty()))
                {
                    boxed(BooleanQuery::with_minimum_required_clauses(v, *min_should))
                } else {
                    boxed(BooleanQuery::new(v))
                }
            }
            SearchQuery::Boost(q, b) => boxed(BoostQuery::new(self.build(q)?, *b)),
            SearchQuery::Keys(keys) => {
                if keys.is_empty() {
                    return Ok(boxed(EmptyQuery));
                }
                boxed(TermSetQuery::new(
                    keys.iter()
                        .map(|k| Term::from_field_bytes(self.schema.key, &k.0)),
                ))
            }
        })
    }
}
