//! `$text`'s `$caseSensitive` and `$diacriticSensitive` (M3, decision
//! S101). The text index matches without case and diacritics; MongoDB then
//! keeps a document only if its indexed text holds the search's terms with
//! the same case (and, with `$diacriticSensitive`, the same diacritics).
//! Here a term holds when a word of the text starts with it under that
//! comparison (so a stemmed form such as "Running" holds "Run"); a phrase
//! holds when the text contains it. As in MongoDB, a document needs one of
//! the terms and every phrase; negated terms and phrases were already
//! excluded by the index without case or diacritics.

use bson::{Bson, Document};
use unicode_normalization::UnicodeNormalization;

/// A `$search` string's positive terms and phrases, normalized for the
/// comparison asked for.
#[derive(Debug)]
pub struct Query {
    terms: Vec<String>,
    phrases: Vec<String>,
    case: bool,
    diacritics: bool,
}

fn fold(s: &str, case: bool, diacritics: bool) -> String {
    let s: String = if diacritics {
        s.to_owned()
    } else {
        s.nfd()
            .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
            .collect()
    };
    if case { s } else { s.to_lowercase() }
}

impl Query {
    /// Parses `$search`.
    #[must_use]
    pub fn parse(search: &str, case: bool, diacritics: bool) -> Self {
        let (mut terms, mut phrases) = (Vec::new(), Vec::new());
        let mut chars = search.chars().peekable();
        let mut word = String::new();
        let mut neg = false;
        let mut push = |w: &mut String, neg: &mut bool, terms: &mut Vec<String>| {
            if !w.is_empty() && !*neg {
                terms.push(fold(w, case, diacritics));
            }
            w.clear();
            *neg = false;
        };
        while let Some(c) = chars.next() {
            match c {
                '"' => {
                    let neg_phrase = neg && word.is_empty();
                    push(&mut word, &mut neg, &mut terms);
                    let mut p = String::new();
                    for c in chars.by_ref() {
                        if c == '"' {
                            break;
                        }
                        p.push(c);
                    }
                    if !neg_phrase && !p.is_empty() {
                        phrases.push(fold(&p, case, diacritics));
                    }
                }
                '-' if word.is_empty() => neg = true,
                c if c.is_whitespace() => push(&mut word, &mut neg, &mut terms),
                c => word.push(c),
            }
        }
        push(&mut word, &mut neg, &mut terms);
        Self {
            terms,
            phrases,
            case,
            diacritics,
        }
    }

    /// Whether the indexed `texts` of a document satisfy the query.
    #[must_use]
    pub fn matches(&self, texts: &[String]) -> bool {
        let texts: Vec<String> = texts
            .iter()
            .map(|t| fold(t, self.case, self.diacritics))
            .collect();
        let phrases_ok = self
            .phrases
            .iter()
            .all(|p| texts.iter().any(|t| t.contains(p.as_str())));
        let term_ok = self.terms.is_empty()
            || self.terms.iter().any(|term| {
                texts.iter().any(|t| {
                    t.split(|c: char| !c.is_alphanumeric())
                        .any(|w| w.starts_with(term.as_str()))
                })
            });
        phrases_ok && term_ok
    }
}

fn collect(v: &Bson, out: &mut Vec<String>) {
    match v {
        Bson::String(s) => out.push(s.clone()),
        Bson::Array(a) => a.iter().for_each(|x| collect(x, out)),
        Bson::Document(d) => d.values().for_each(|x| collect(x, out)),
        _ => {}
    }
}

/// The strings of `d` under the text index's `fields` (`$**`: all).
#[must_use]
pub fn texts(d: &Document, fields: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for f in fields {
        if f == "$**" {
            d.values().for_each(|v| collect(v, &mut out));
        } else {
            for v in crate::filter::values_at(d, f).leaves {
                collect(v, &mut out);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_and_diacritics() {
        let t = vec!["Le Café Running".to_owned()];
        assert!(Query::parse("Run", true, false).matches(&t));
        assert!(!Query::parse("run", true, false).matches(&t));
        assert!(Query::parse("cafe", false, false).matches(&t));
        assert!(!Query::parse("cafe", false, true).matches(&t));
        assert!(Query::parse("café", false, true).matches(&t));
        assert!(Query::parse("\"Le Café\"", true, true).matches(&t));
        assert!(!Query::parse("\"le café\"", true, true).matches(&t));
    }
}
