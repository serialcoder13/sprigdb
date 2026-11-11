//! Analyzers (chapter 39, "Analyzers"; build step 2).
//!
//! An analyzer is a Tantivy [`TextAnalyzer`] chain, registered in each
//! partition's tokenizer manager under a versioned name, and run again at
//! query time (on the query text) and for highlights (on fetched text).
//!
//! | Built-in | Chain |
//! |---|---|
//! | `standard` | simple tokenizer, tokens over 255 bytes removed, lowercase |
//! | `english`, `french`, ... | standard, the language's stop words, its Snowball stemmer |
//! | `keyword` | the whole value |
//! | `whitespace` | whitespace tokenizer |
//! | `autocomplete` | standard, then edge n-grams of 2–15 characters (index time) |
//! | `trigram` | n-grams of 3, lowercase, ASCII folding |
//! | `mysql_compat` | standard, English stop words, tokens of 3 characters or more |
//! | `mongo_<language>` | standard, ASCII folding, the language's stop words and stemmer |
//!
//! Stop words and stemmers are the ones Tantivy ships for each language,
//! referenced by name: SprigDB keeps no word lists of its own.
//! `mysql_compat` and the MongoDB `$text` analyzers therefore use Tantivy's
//! stop words, not `InnoDB`'s or `MongoDB`'s lists; a query made only of words
//! one list has and the other doesn't can match differently.
//!
//! **Synonyms** are applied at query time ([`Analyzer::expand`]): each
//! query token becomes an OR of its set, so changing synonyms needs no
//! rebuild.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigCode, SprigError};
use tantivy::tokenizer::{
    AsciiFoldingFilter, Language, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter,
    SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer, Token, TokenFilter, TokenStream,
    Tokenizer, WhitespaceTokenizer,
};

/// Longest token kept, in bytes.
pub const MAX_TOKEN_BYTES: usize = 255;
/// Edge n-grams of `autocomplete`.
pub const EDGE_MIN: usize = 2;
/// Edge n-grams of `autocomplete`.
pub const EDGE_MAX: usize = 15;
/// Shortest token `mysql_compat` keeps (`InnoDB`'s `innodb_ft_min_token_size`).
pub const MYSQL_MIN_TOKEN: usize = 3;

fn unknown(name: &str) -> SprigError {
    SprigError::new(
        SprigCode::SearchAnalyzerUnknown,
        format!("search analyzer or language '{name}' is not defined"),
    )
}

/// A language by name (English names, ISO 639-1 codes, MongoDB's names).
#[must_use]
pub fn language(name: &str) -> Option<Language> {
    Some(match name.to_ascii_lowercase().as_str() {
        "arabic" | "ar" => Language::Arabic,
        "danish" | "da" => Language::Danish,
        "dutch" | "nl" => Language::Dutch,
        "english" | "en" => Language::English,
        "finnish" | "fi" => Language::Finnish,
        "french" | "fr" => Language::French,
        "german" | "de" => Language::German,
        "greek" | "el" => Language::Greek,
        "hungarian" | "hu" => Language::Hungarian,
        "italian" | "it" => Language::Italian,
        "norwegian" | "nb" | "no" => Language::Norwegian,
        "portuguese" | "pt" => Language::Portuguese,
        "romanian" | "ro" => Language::Romanian,
        "russian" | "ru" => Language::Russian,
        "spanish" | "es" => Language::Spanish,
        "swedish" | "sv" => Language::Swedish,
        "tamil" | "ta" => Language::Tamil,
        "turkish" | "tr" => Language::Turkish,
        _ => return None,
    })
}

/// An Atlas analyzer name (`lucene.english`) as SprigDB's.
#[must_use]
pub fn from_atlas(name: &str) -> String {
    match name.strip_prefix("lucene.") {
        Some("standard" | "simple") => "standard".to_owned(),
        Some(l) => l.to_owned(),
        None => name.to_owned(),
    }
}

/// A user-defined analyzer (`CREATE SEARCH ANALYZER`; catalog
/// `search_analyzers`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomAnalyzer {
    /// Its name.
    pub name: String,
    /// `standard`, `whitespace`, `keyword` (`raw`) or `ngram`.
    pub tokenizer: String,
    /// `{"min_gram": 3, "max_gram": 3}` for `ngram`.
    #[serde(default)]
    pub tokenizer_opts: serde_json::Value,
    /// In order: `lowercase`, `ascii_folding`, `stop:<language>`,
    /// `stem:<language>`, `edge_ngram`, `min_length:<n>`, `max_length:<n>`.
    pub filters: Vec<String>,
    /// Sets of equivalent words, applied at query time.
    #[serde(default)]
    pub synonyms: Vec<Vec<String>>,
    /// Raised on every change; part of the registered name.
    pub version: u32,
}

/// One analyzer, ready to run.
#[derive(Clone)]
pub struct Analyzer {
    /// Its name.
    pub name: String,
    /// The name it is registered under in partitions (`name` plus the
    /// version of a custom analyzer).
    pub registered: String,
    index: TextAnalyzer,
    query: TextAnalyzer,
    /// Token → its synonym set (all analyzed with `query`).
    synonyms: HashMap<String, Vec<String>>,
}

impl std::fmt::Debug for Analyzer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Analyzer")
            .field("name", &self.name)
            .field("registered", &self.registered)
            .finish_non_exhaustive()
    }
}

/// A token with its position and byte offsets in the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tok {
    /// The analyzed text.
    pub text: String,
    /// Position (for phrases).
    pub position: usize,
    /// Byte offsets in the input.
    pub from: usize,
    /// Byte offsets in the input.
    pub to: usize,
}

fn run(a: &mut TextAnalyzer, text: &str) -> Vec<Tok> {
    let mut s = a.token_stream(text);
    let mut out = Vec::new();
    while s.advance() {
        let t = s.token();
        out.push(Tok {
            text: t.text.clone(),
            position: t.position,
            from: t.offset_from,
            to: t.offset_to,
        });
    }
    out
}

impl Analyzer {
    /// The index-time chain.
    #[must_use]
    pub fn index_analyzer(&self) -> TextAnalyzer {
        self.index.clone()
    }

    /// Tokens of `text` as indexed.
    #[must_use]
    pub fn index_tokens(&self, text: &str) -> Vec<Tok> {
        run(&mut self.index.clone(), text)
    }

    /// Tokens of a query text.
    #[must_use]
    pub fn query_tokens(&self, text: &str) -> Vec<Tok> {
        run(&mut self.query.clone(), text)
    }

    /// A query token and its synonyms (the token first).
    #[must_use]
    pub fn expand(&self, token: &str) -> Vec<String> {
        match self.synonyms.get(token) {
            Some(set) => {
                let mut v = vec![token.to_owned()];
                v.extend(set.iter().filter(|s| *s != token).cloned());
                v
            }
            None => vec![token.to_owned()],
        }
    }

    fn plain(name: &str, index: TextAnalyzer, query: Option<TextAnalyzer>) -> Self {
        Self {
            name: name.to_owned(),
            registered: name.to_owned(),
            query: query.unwrap_or_else(|| index.clone()),
            index,
            synonyms: HashMap::new(),
        }
    }
}

fn standard() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES))
        .filter(LowerCaser)
        .build()
}

fn stop(lang: Language) -> Option<StopWordFilter> {
    StopWordFilter::new(lang)
}

/// A language's chain. `fold` (MongoDB's text indexes) folds diacritics
/// after stemming, as MongoDB does: `café` matches `cafe`, while `résumé`
/// stems as written and doesn't match `resume`.
fn language_chain(lang: Language, fold: bool) -> TextAnalyzer {
    let b = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES))
        .filter(LowerCaser)
        .dynamic();
    let b = match stop(lang) {
        Some(s) => b.filter_dynamic(s),
        None => b,
    };
    let b = b.filter_dynamic(Stemmer::new(lang));
    if fold {
        b.filter_dynamic(AsciiFoldingFilter).build()
    } else {
        b.build()
    }
}

/// A built-in analyzer by name.
///
/// # Errors
/// 39828 for an unknown name.
pub fn builtin(name: &str) -> Result<Analyzer> {
    let n = from_atlas(name);
    Ok(match n.as_str() {
        "standard" => Analyzer::plain(&n, standard(), None),
        "keyword" | "raw" => Analyzer::plain(&n, TextAnalyzer::from(RawTokenizer::default()), None),
        "whitespace" => {
            Analyzer::plain(&n, TextAnalyzer::from(WhitespaceTokenizer::default()), None)
        }
        "autocomplete" => Analyzer::plain(
            &n,
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES))
                .filter(LowerCaser)
                .filter(EdgeNgram::new(EDGE_MIN, EDGE_MAX))
                .build(),
            Some(standard()),
        ),
        "trigram" => Analyzer::plain(
            &n,
            TextAnalyzer::builder(
                NgramTokenizer::new(3, 3, false)
                    .map_err(|e| SprigError::internal(format!("trigram tokenizer: {e}")))?,
            )
            .filter(LowerCaser)
            .filter(AsciiFoldingFilter)
            .build(),
            None,
        ),
        "mysql_compat" => {
            let b = TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES))
                .filter(LowerCaser)
                .filter(MinLength(MYSQL_MIN_TOKEN))
                .dynamic();
            let b = match stop(Language::English) {
                Some(s) => b.filter_dynamic(s),
                None => b,
            };
            Analyzer::plain(&n, b.build(), None)
        }
        other => {
            if let Some(l) = other.strip_prefix("mongo_") {
                if l == "none" {
                    let a = TextAnalyzer::builder(SimpleTokenizer::default())
                        .filter(RemoveLongFilter::limit(MAX_TOKEN_BYTES))
                        .filter(LowerCaser)
                        .filter(AsciiFoldingFilter)
                        .build();
                    return Ok(Analyzer::plain(other, a, None));
                }
                let lang = language(l).ok_or_else(|| unknown(name))?;
                Analyzer::plain(other, language_chain(lang, true), None)
            } else {
                let lang = language(other).ok_or_else(|| unknown(name))?;
                Analyzer::plain(other, language_chain(lang, false), None)
            }
        }
    })
}

/// Whether `name` is a built-in analyzer.
#[must_use]
pub fn is_builtin(name: &str) -> bool {
    builtin(name).is_ok()
}

fn custom_chain(c: &CustomAnalyzer, query_side: bool) -> Result<TextAnalyzer> {
    let mut b = match c.tokenizer.as_str() {
        "standard" => TextAnalyzer::builder(SimpleTokenizer::default()).dynamic(),
        "whitespace" => TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic(),
        "keyword" | "raw" => TextAnalyzer::builder(RawTokenizer::default()).dynamic(),
        "ngram" => {
            let min = c.tokenizer_opts["min_gram"].as_u64().unwrap_or(3);
            let max = c.tokenizer_opts["max_gram"].as_u64().unwrap_or(min);
            let min = usize::try_from(min).unwrap_or(3);
            let max = usize::try_from(max).unwrap_or(min);
            TextAnalyzer::builder(
                NgramTokenizer::new(min, max, false)
                    .map_err(|e| unknown(&format!("ngram tokenizer ({e})")))?,
            )
            .dynamic()
        }
        other => return Err(unknown(&format!("tokenizer {other}"))),
    };
    for f in &c.filters {
        let (kind, arg) = f.split_once(':').unwrap_or((f.as_str(), ""));
        b = match kind {
            "lowercase" => b.filter_dynamic(LowerCaser),
            "ascii_folding" => b.filter_dynamic(AsciiFoldingFilter),
            "stop" => {
                let l = language(arg).ok_or_else(|| unknown(arg))?;
                match stop(l) {
                    Some(s) => b.filter_dynamic(s),
                    None => return Err(unknown(&format!("stop words for {arg}"))),
                }
            }
            "stem" => b.filter_dynamic(Stemmer::new(language(arg).ok_or_else(|| unknown(arg))?)),
            "min_length" => b.filter_dynamic(MinLength(arg.parse().map_err(|_| unknown(f))?)),
            "max_length" => b.filter_dynamic(RemoveLongFilter::limit(
                arg.parse().map_err(|_| unknown(f))?,
            )),
            // The query side stops before the n-grams.
            "edge_ngram" if query_side => break,
            "edge_ngram" => b.filter_dynamic(EdgeNgram::new(EDGE_MIN, EDGE_MAX)),
            _ => return Err(unknown(&format!("filter {f}"))),
        };
    }
    Ok(b.build())
}

/// Builds a custom analyzer.
///
/// # Errors
/// 39828 for an unknown tokenizer, filter or language.
pub fn custom(c: &CustomAnalyzer) -> Result<Analyzer> {
    let mut a = Analyzer {
        name: c.name.clone(),
        registered: format!("{}~v{}", c.name, c.version),
        index: custom_chain(c, false)?,
        query: custom_chain(c, true)?,
        synonyms: HashMap::new(),
    };
    let mut q = a.query.clone();
    for set in &c.synonyms {
        let analyzed: Vec<String> = set
            .iter()
            .filter_map(|w| run(&mut q, w).into_iter().next().map(|t| t.text))
            .collect();
        for w in &analyzed {
            a.synonyms.insert(w.clone(), analyzed.clone());
        }
    }
    Ok(a)
}

/// The analyzers an index uses, by name.
#[derive(Debug, Clone, Default)]
pub struct Analyzers {
    by_name: BTreeMap<String, Analyzer>,
}

impl Analyzers {
    /// The built-ins `names` need and the custom analyzers `custom`.
    ///
    /// # Errors
    /// 39828 for a name that is neither.
    pub fn resolve<'a>(
        names: impl IntoIterator<Item = &'a str>,
        customs: &[CustomAnalyzer],
    ) -> Result<Self> {
        let mut by_name = BTreeMap::new();
        for n in names {
            if by_name.contains_key(n) {
                continue;
            }
            let a = match customs.iter().find(|c| c.name == n) {
                Some(c) => custom(c)?,
                None => builtin(n)?,
            };
            by_name.insert(n.to_owned(), a);
        }
        Ok(Self { by_name })
    }

    /// An analyzer by name.
    ///
    /// # Errors
    /// 39828 when it isn't resolved.
    pub fn get(&self, name: &str) -> Result<&Analyzer> {
        self.by_name.get(name).ok_or_else(|| unknown(name))
    }

    /// Every analyzer.
    pub fn iter(&self) -> impl Iterator<Item = &Analyzer> {
        self.by_name.values()
    }
}

// ---- Two token filters Tantivy doesn't have ----

/// Keeps tokens of at least `n` characters.
#[derive(Clone, Copy, Debug)]
pub struct MinLength(pub usize);

impl TokenFilter for MinLength {
    type Tokenizer<T: Tokenizer> = MinLengthTok<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> MinLengthTok<T> {
        MinLengthTok {
            n: self.0,
            inner: tokenizer,
        }
    }
}

/// [`MinLength`] around a tokenizer.
#[derive(Clone, Debug)]
pub struct MinLengthTok<T> {
    n: usize,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for MinLengthTok<T> {
    type TokenStream<'a> = MinLengthStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        MinLengthStream {
            n: self.n,
            tail: self.inner.token_stream(text),
        }
    }
}

/// [`MinLength`]'s stream.
#[derive(Debug)]
pub struct MinLengthStream<S> {
    n: usize,
    tail: S,
}

impl<S: TokenStream> TokenStream for MinLengthStream<S> {
    fn advance(&mut self) -> bool {
        while self.tail.advance() {
            if self.tail.token().text.chars().count() >= self.n {
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

/// Replaces each token by its prefixes of `min..=max` characters, at the
/// token's position (a token shorter than `min` stays whole).
#[derive(Clone, Copy, Debug)]
pub struct EdgeNgram {
    min: usize,
    max: usize,
}

impl EdgeNgram {
    /// Prefixes of `min..=max` characters.
    #[must_use]
    pub fn new(min: usize, max: usize) -> Self {
        Self {
            min: min.max(1),
            max: max.max(min.max(1)),
        }
    }
}

impl TokenFilter for EdgeNgram {
    type Tokenizer<T: Tokenizer> = EdgeNgramTok<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> EdgeNgramTok<T> {
        EdgeNgramTok {
            f: self,
            inner: tokenizer,
        }
    }
}

/// [`EdgeNgram`] around a tokenizer.
#[derive(Clone, Debug)]
pub struct EdgeNgramTok<T> {
    f: EdgeNgram,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for EdgeNgramTok<T> {
    type TokenStream<'a> = EdgeNgramStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        EdgeNgramStream {
            f: self.f,
            tail: self.inner.token_stream(text),
            base: Token::default(),
            ends: Vec::new(),
            next: 0,
            cur: Token::default(),
        }
    }
}

/// [`EdgeNgram`]'s stream.
#[derive(Debug)]
pub struct EdgeNgramStream<S> {
    f: EdgeNgram,
    tail: S,
    base: Token,
    /// Byte ends of the prefixes still to emit.
    ends: Vec<usize>,
    next: usize,
    cur: Token,
}

impl<S: TokenStream> TokenStream for EdgeNgramStream<S> {
    fn advance(&mut self) -> bool {
        loop {
            if self.next < self.ends.len() {
                let end = self.ends[self.next];
                self.next += 1;
                self.cur = self.base.clone();
                self.cur.text.truncate(end);
                return true;
            }
            if !self.tail.advance() {
                return false;
            }
            self.base = self.tail.token().clone();
            let t = &self.base.text;
            let bounds: Vec<usize> = t.char_indices().map(|(i, c)| i + c.len_utf8()).collect();
            self.ends = if bounds.len() < self.f.min {
                vec![t.len()]
            } else {
                bounds
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i + 1 >= self.f.min && *i < self.f.max)
                    .map(|(_, b)| *b)
                    .collect()
            };
            self.next = 0;
        }
    }

    fn token(&self) -> &Token {
        &self.cur
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.cur
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(a: &Analyzer, s: &str) -> Vec<String> {
        a.index_tokens(s).into_iter().map(|t| t.text).collect()
    }

    #[test]
    fn standard_lowercases_and_splits() {
        let a = builtin("standard").unwrap();
        assert_eq!(texts(&a, "Red Running-Shoes"), ["red", "running", "shoes"]);
    }

    #[test]
    fn english_stems_and_drops_stop_words() {
        let a = builtin("lucene.english").unwrap();
        assert_eq!(texts(&a, "The runners are running"), ["runner", "run"]);
    }

    #[test]
    fn mongo_english_folds_diacritics() {
        let a = builtin("mongo_english").unwrap();
        assert_eq!(texts(&a, "Café crème"), ["cafe", "creme"]);
    }

    #[test]
    fn autocomplete_indexes_edge_ngrams_and_queries_whole_words() {
        let a = builtin("autocomplete").unwrap();
        assert_eq!(texts(&a, "Shoe"), ["sh", "sho", "shoe"]);
        let q: Vec<String> = a.query_tokens("Sho").into_iter().map(|t| t.text).collect();
        assert_eq!(q, ["sho"]);
    }

    #[test]
    fn trigram_and_min_length() {
        let a = builtin("trigram").unwrap();
        assert_eq!(texts(&a, "ABCd"), ["abc", "bcd"]);
        let m = builtin("mysql_compat").unwrap();
        assert_eq!(texts(&m, "an ox ran far away"), ["ran", "far", "away"]);
    }

    #[test]
    fn custom_with_synonyms() {
        let c = CustomAnalyzer {
            name: "en_syn".into(),
            tokenizer: "standard".into(),
            tokenizer_opts: serde_json::Value::Null,
            filters: vec!["lowercase".into(), "stem:english".into()],
            synonyms: vec![vec!["tv".into(), "television".into()]],
            version: 1,
        };
        let a = custom(&c).unwrap();
        assert_eq!(a.registered, "en_syn~v1");
        assert_eq!(a.expand("tv"), ["tv", "televis"]);
        assert_eq!(a.expand("radio"), ["radio"]);
    }

    #[test]
    fn unknown_names_are_39828() {
        let e = builtin("klingon").unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::SearchAnalyzerUnknown));
    }
}
