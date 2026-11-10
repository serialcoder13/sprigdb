//! Collations (chapter 31, "Collations"; R5 round 4).
//!
//! A command's `collation` option (`find`, `aggregate`, `count`,
//! `distinct`, the writes' statements, `findAndModify`) changes how
//! strings compare: [`Collation::compare`] folds them as ICU's root
//! collation does at the requested strength, in the gateway. The command
//! runs inside [`scope`], a task-local value that
//! [`crate::keyenc::compare`] reads, so every comparison the command makes
//! (filters, sorts, `distinct`, `$in`, ranges) follows it without each
//! function taking a parameter. The SQL translation pushes no string
//! comparison down while a collation is active ([`crate::translate`]),
//! since MariaDB's `utf8mb4_bin` would leave out rows the collation
//! matches; such queries read their candidates by the other conditions
//! and recheck in the gateway.
//!
//! What it covers: strength 1 (base letters: case and accents ignored),
//! 2 (accents count, case ignored), 3 to 5 (case counts, lower case first
//! unless `caseFirst: "upper"`), `caseLevel`, and `numericOrdering`
//! (digit runs compare as numbers). The locale's own tailoring is not
//! applied: every locale compares as the root locale ("simple" means no
//! collation). `alternate`, `maxVariable`, `backwards` and
//! `normalization` are accepted and have no effect.

use std::cmp::Ordering;
use std::future::Future;

use bson::{Bson, Document};
use sprig_proto_mongo::errors::{MongoError, code};
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::is_combining_mark;

/// A collation's comparison rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Collation {
    /// ICU strength, 1 to 5.
    pub strength: u8,
    /// `caseLevel`: case counts even at strength 1 and 2.
    pub case_level: bool,
    /// `numericOrdering`.
    pub numeric: bool,
    /// `caseFirst: "upper"`.
    pub upper_first: bool,
}

tokio::task_local! {
    static ACTIVE: Collation;
}

/// The collation of the command running on this task, if any.
#[must_use]
pub fn active() -> Option<Collation> {
    ACTIVE.try_with(|c| *c).ok()
}

/// Runs `f` with `c` as the active collation (none: as it is).
pub async fn scope<F: Future>(c: Option<Collation>, f: F) -> F::Output {
    match c {
        Some(c) => ACTIVE.scope(c, f).await,
        None => f.await,
    }
}

/// Runs the synchronous `f` with `c` as the active collation.
pub fn sync_scope<R>(c: Option<Collation>, f: impl FnOnce() -> R) -> R {
    match c {
        Some(c) => ACTIVE.sync_scope(c, f),
        None => f(),
    }
}

fn bad(m: impl Into<String>) -> MongoError {
    MongoError::new(code::BAD_VALUE, m)
}

impl Collation {
    /// Parses a `collation` option: `None` for the simple collation
    /// (`{locale: "simple"}`).
    ///
    /// # Errors
    /// 2 `BadValue` for a missing locale or an invalid option.
    pub fn parse(d: &Document) -> Result<Option<Self>, MongoError> {
        let locale = match d.get("locale") {
            Some(Bson::String(s)) => s.as_str(),
            Some(_) => return Err(bad("collation locale must be a string")),
            None => return Err(bad("Missing expected field \"locale\"")),
        };
        let mut c = Self {
            strength: 3,
            case_level: false,
            numeric: false,
            upper_first: false,
        };
        for (k, v) in d {
            match (k.as_str(), v) {
                ("strength", v) => {
                    let n = crate::filter::as_i64(v)
                        .filter(|n| (1..=5).contains(n))
                        .ok_or_else(|| bad("collation strength must be an integer from 1 to 5"))?;
                    c.strength = u8::try_from(n).unwrap_or(3);
                }
                ("caseLevel", Bson::Boolean(b)) => c.case_level = *b,
                ("numericOrdering", Bson::Boolean(b)) => c.numeric = *b,
                ("caseFirst", Bson::String(s)) => match s.as_str() {
                    "upper" => c.upper_first = true,
                    "lower" | "off" => {}
                    _ => return Err(bad(format!("unsupported caseFirst value: {s}"))),
                },
                ("alternate", Bson::String(s))
                    if matches!(s.as_str(), "non-ignorable" | "shifted") => {}
                ("maxVariable", Bson::String(s)) if matches!(s.as_str(), "punct" | "space") => {}
                ("locale", _)
                | ("backwards" | "normalization", Bson::Boolean(_))
                | ("version", Bson::String(_)) => {}
                _ => return Err(bad(format!("unknown or invalid collation field: {k}"))),
            }
        }
        if locale == "simple" {
            if d.len() > 1 {
                return Err(bad(
                    "If the locale is \"simple\", no other collation options may be specified",
                ));
            }
            return Ok(None);
        }
        Ok(Some(c))
    }

    /// The collation of a command or statement document's `collation`
    /// field.
    ///
    /// # Errors
    /// As [`Self::parse`]; 2 when the field isn't a document.
    pub fn of(d: &Document) -> Result<Option<Self>, MongoError> {
        match d.get("collation") {
            None => Ok(None),
            Some(Bson::Document(c)) => Self::parse(c),
            Some(_) => Err(MongoError::new(
                code::TYPE_MISMATCH,
                "collation must be an object",
            )),
        }
    }

    /// Compares two strings under this collation.
    #[must_use]
    pub fn compare(self, a: &str, b: &str) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        let (ka, kb) = (Keys::of(a), Keys::of(b));
        let o = self.cmp_level(&ka.primary, &kb.primary);
        if o != Ordering::Equal {
            return o;
        }
        if self.case_level {
            let o = self.cmp_case(&ka.case, &kb.case);
            if o != Ordering::Equal {
                return o;
            }
        }
        if self.strength >= 2 {
            let o = self.cmp_level(&ka.secondary, &kb.secondary);
            if o != Ordering::Equal {
                return o;
            }
        }
        if self.strength >= 3 {
            let o = self.cmp_case(&ka.case, &kb.case);
            if o != Ordering::Equal {
                return o;
            }
        }
        if self.strength >= 4 {
            // Identical level: the code points.
            return a.nfd().cmp(b.nfd());
        }
        Ordering::Equal
    }

    fn cmp_level(self, a: &[char], b: &[char]) -> Ordering {
        if self.numeric {
            numeric_cmp(a, b)
        } else {
            a.cmp(b)
        }
    }

    /// Lower case before upper case unless `caseFirst: "upper"`.
    fn cmp_case(self, a: &[bool], b: &[bool]) -> Ordering {
        let o = a.cmp(b);
        if self.upper_first { o.reverse() } else { o }
    }
}

/// A string's collation levels: base letters, letters with accents, and
/// which letters are upper case.
struct Keys {
    primary: Vec<char>,
    secondary: Vec<char>,
    case: Vec<bool>,
}

impl Keys {
    fn of(s: &str) -> Self {
        let mut primary = Vec::with_capacity(s.len());
        let mut secondary = Vec::with_capacity(s.len());
        let mut case = Vec::with_capacity(s.len());
        for ch in s.nfd() {
            let upper = ch.is_uppercase();
            for l in ch.to_lowercase() {
                secondary.push(l);
                if !is_combining_mark(l) {
                    primary.push(l);
                    case.push(upper);
                }
            }
        }
        Self {
            primary,
            secondary,
            case,
        }
    }
}

/// Compares with runs of ASCII digits as numbers (`numericOrdering`).
fn numeric_cmp(a: &[char], b: &[char]) -> Ordering {
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a[i].is_ascii_digit() && b[j].is_ascii_digit() {
            let si = i;
            while i < a.len() && a[i].is_ascii_digit() {
                i += 1;
            }
            let sj = j;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            let na = trim_zeros(&a[si..i]);
            let nb = trim_zeros(&b[sj..j]);
            let o = na.len().cmp(&nb.len()).then_with(|| na.cmp(nb));
            if o != Ordering::Equal {
                return o;
            }
        } else {
            let o = a[i].cmp(&b[j]);
            if o != Ordering::Equal {
                return o;
            }
            i += 1;
            j += 1;
        }
    }
    (a.len() - i).cmp(&(b.len() - j))
}

fn trim_zeros(d: &[char]) -> &[char] {
    let n = d.iter().take_while(|c| **c == '0').count();
    &d[n.min(d.len().saturating_sub(1))..]
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    fn c(d: &Document) -> Collation {
        Collation::parse(d).unwrap().unwrap()
    }

    #[test]
    fn strengths() {
        let s1 = c(&doc! {"locale": "en_US", "strength": 1});
        let s2 = c(&doc! {"locale": "en_US", "strength": 2});
        let s3 = c(&doc! {"locale": "en_US"});
        assert_eq!(s1.compare("ping", "PING"), Ordering::Equal);
        assert_eq!(s1.compare("café", "CAFE"), Ordering::Equal);
        assert_eq!(s2.compare("ping", "PING"), Ordering::Equal);
        assert_ne!(s2.compare("café", "cafe"), Ordering::Equal);
        assert_eq!(s3.compare("a", "A"), Ordering::Less);
        assert_eq!(s3.compare("A", "b"), Ordering::Less);
        assert_eq!(s3.compare("ping", "pong"), Ordering::Less);
        let up = c(&doc! {"locale": "en", "caseFirst": "upper"});
        assert_eq!(up.compare("a", "A"), Ordering::Greater);
    }

    #[test]
    fn numeric_ordering() {
        let n = c(&doc! {"locale": "en", "numericOrdering": true});
        assert_eq!(n.compare("item10", "item9"), Ordering::Greater);
        assert_eq!(n.compare("item010", "item10"), Ordering::Equal);
        let plain = c(&doc! {"locale": "en"});
        assert_eq!(plain.compare("item10", "item9"), Ordering::Less);
    }

    #[test]
    fn simple_and_errors() {
        assert!(
            Collation::parse(&doc! {"locale": "simple"})
                .unwrap()
                .is_none()
        );
        assert!(Collation::parse(&doc! {"strength": 1}).is_err());
        assert!(Collation::parse(&doc! {"locale": "en", "strength": 9}).is_err());
        assert!(Collation::parse(&doc! {"locale": "en", "bogus": 1}).is_err());
    }

    #[tokio::test]
    async fn the_scope_reaches_keyenc() {
        let ci = c(&doc! {"locale": "en", "strength": 2});
        let eq = scope(Some(ci), async {
            crate::keyenc::equal(&Bson::String("ping".into()), &Bson::String("PING".into()))
        })
        .await;
        assert!(eq);
        assert!(!crate::keyenc::equal(
            &Bson::String("ping".into()),
            &Bson::String("PING".into())
        ));
    }
}
