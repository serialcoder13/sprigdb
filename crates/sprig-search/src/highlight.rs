//! Highlights (chapter 39, "Highlights and facets"; build step 8).
//!
//! Text is never stored in the index: the gateway highlights the text the
//! lookup fetched from MariaDB. The field's analyzer runs over it, tokens
//! matching a query term (or an expansion of one) are marked, and the
//! fragment of about `chars` characters with the most distinct hits is
//! returned with the hits' byte ranges inside it.

use std::collections::HashSet;

use serde::Serialize;

use crate::analyze::Analyzer;

/// One highlighted fragment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Fragment {
    /// The text.
    pub text: String,
    /// Byte ranges of hits inside `text`.
    pub hits: Vec<(usize, usize)>,
}

/// The best fragment of `text` for `terms` (analyzed), or `None` when no
/// token matches.
#[must_use]
pub fn fragment<S: std::hash::BuildHasher>(
    analyzer: &Analyzer,
    text: &str,
    terms: &HashSet<String, S>,
    chars: usize,
) -> Option<Fragment> {
    let toks: Vec<_> = analyzer
        .index_tokens(text)
        .into_iter()
        .filter(|t| terms.contains(&t.text))
        .collect();
    if toks.is_empty() {
        return None;
    }
    let width = chars.max(20);
    // The window starting at each hit; the one with most distinct terms wins.
    let mut best = (0usize, 0usize);
    for (i, t) in toks.iter().enumerate() {
        let mut seen = HashSet::new();
        for u in &toks[i..] {
            if text[t.from..u.to].chars().count() > width {
                break;
            }
            seen.insert(u.text.as_str());
        }
        if seen.len() > best.1 {
            best = (i, seen.len());
        }
    }
    let start_tok = &toks[best.0];
    // Start a little before the first hit, on a char boundary.
    let mut start = start_tok.from;
    let lead = width / 5;
    let mut back = 0;
    while start > 0 && back < lead {
        start -= 1;
        while !text.is_char_boundary(start) {
            start -= 1;
        }
        back += 1;
    }
    let mut end = start;
    for (n, (i, c)) in text[start..].char_indices().enumerate() {
        if n == width {
            end = start + i;
            break;
        }
        end = start + i + c.len_utf8();
    }
    let hits = toks
        .iter()
        .filter(|t| t.from >= start && t.to <= end)
        .map(|t| (t.from - start, t.to - start))
        .collect();
    Some(Fragment {
        text: text[start..end].to_owned(),
        hits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::builtin;

    #[test]
    fn marks_stemmed_hits() {
        let a = builtin("english").unwrap();
        let terms: HashSet<String> = ["run".to_owned()].into();
        let f = fragment(&a, "Shoes for Running and more running", &terms, 150).unwrap();
        assert_eq!(f.hits.len(), 2);
        assert_eq!(&f.text[f.hits[0].0..f.hits[0].1], "Running");
        assert!(fragment(&a, "nothing here", &terms, 150).is_none());
    }
}
