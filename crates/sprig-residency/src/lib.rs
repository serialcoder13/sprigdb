//! Residency policies (chapter 26, "Interfaces" and "Evaluating a
//! policy"; build step 1): a condition over DC tags. A crate of its own
//! (R9, r9more) so gateways check sink destinations at `CREATE CONNECTION`
//! without linking the controller role.
//!
//! ```text
//! policy   := or_expr                               # empty policy = every DC
//! or_expr  := and_expr { "or" and_expr }
//! and_expr := unary { "and" unary }
//! unary    := "not" unary | "(" or_expr ")" | pred
//! pred     := name "=" str | name "!=" str | name "in" "(" str { "," str } ")"
//! name     := "dc" | tag key matching [a-z][a-z0-9_]{0,31}
//! str      := double-quoted UTF-8 text
//! ```
//!
//! `admits` evaluates against the DC's tags plus the pseudo-tag `dc` (its
//! id). `name = s` needs the tag present and equal; `name != s` holds when
//! the tag is missing or different; `name in (...)` needs it present and
//! listed. Keywords (`and`, `or`, `not`, `in`) are lower case; a string
//! takes `\"` and `\\` escapes. Tag values that aren't JSON strings
//! compare by their JSON text.

use std::collections::BTreeSet;
use std::fmt;

use serde_json::Value;
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::bootstrap::DcRecord;

/// The longest policy text accepted, in bytes.
pub const MAX_POLICY_LEN: usize = 4096;
/// The deepest nesting accepted.
const MAX_DEPTH: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Expr {
    Or(Vec<Expr>),
    And(Vec<Expr>),
    Not(Box<Expr>),
    Eq(String, String),
    Ne(String, String),
    In(String, Vec<String>),
}

/// A parsed policy. The empty policy admits every DC.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Policy {
    text: String,
    ast: Option<Expr>,
}

fn invalid(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ResidencyInvalid, msg)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Word(String),
    Str(String),
    Eq,
    Ne,
    LParen,
    RParen,
    Comma,
}

#[allow(clippy::many_single_char_names)] // a byte lexer: b, i, c
fn lex(text: &str) -> Result<Vec<(usize, Tok)>> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            b'(' => {
                out.push((i, Tok::LParen));
                i += 1;
            }
            b')' => {
                out.push((i, Tok::RParen));
                i += 1;
            }
            b',' => {
                out.push((i, Tok::Comma));
                i += 1;
            }
            b'=' => {
                out.push((i, Tok::Eq));
                i += 1;
            }
            b'!' if b.get(i + 1) == Some(&b'=') => {
                out.push((i, Tok::Ne));
                i += 2;
            }
            b'"' => {
                let start = i;
                i += 1;
                let mut s = Vec::new();
                loop {
                    match b.get(i) {
                        None => {
                            return Err(invalid(format!(
                                "unterminated string at byte {start} of the policy"
                            )));
                        }
                        Some(b'"') => {
                            i += 1;
                            break;
                        }
                        Some(b'\\') => match b.get(i + 1) {
                            Some(&e @ (b'"' | b'\\')) => {
                                s.push(e);
                                i += 2;
                            }
                            _ => {
                                return Err(invalid(format!(
                                    "bad escape at byte {i} of the policy: only \\\" and \\\\"
                                )));
                            }
                        },
                        Some(&x) => {
                            s.push(x);
                            i += 1;
                        }
                    }
                }
                let s = String::from_utf8(s).map_err(|_| invalid("a policy string isn't UTF-8"))?;
                out.push((start, Tok::Str(s)));
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                out.push((start, Tok::Word(text[start..i].to_owned())));
            }
            _ => {
                return Err(invalid(format!(
                    "unexpected {:?} at byte {i} of the policy",
                    char::from(c)
                )));
            }
        }
    }
    Ok(out)
}

/// Whether `s` is a tag key: `[a-z][a-z0-9_]{0,31}`.
#[must_use]
pub fn valid_tag_key(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 32
        && b[0].is_ascii_lowercase()
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

fn keyword(w: &str) -> bool {
    matches!(w, "and" | "or" | "not" | "in")
}

struct Parser {
    toks: Vec<(usize, Tok)>,
    pos: usize,
    depth: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|(_, t)| t)
    }

    fn at(&self) -> String {
        self.toks
            .get(self.pos)
            .map_or_else(|| "the end".to_owned(), |(i, _)| format!("byte {i}"))
    }

    fn word(&self, w: &str) -> bool {
        matches!(self.peek(), Some(Tok::Word(x)) if x == w)
    }

    fn or_expr(&mut self) -> Result<Expr> {
        let mut v = vec![self.and_expr()?];
        while self.word("or") {
            self.pos += 1;
            v.push(self.and_expr()?);
        }
        Ok(if v.len() == 1 {
            v.remove(0)
        } else {
            Expr::Or(v)
        })
    }

    fn and_expr(&mut self) -> Result<Expr> {
        let mut v = vec![self.unary()?];
        while self.word("and") {
            self.pos += 1;
            v.push(self.unary()?);
        }
        Ok(if v.len() == 1 {
            v.remove(0)
        } else {
            Expr::And(v)
        })
    }

    fn unary(&mut self) -> Result<Expr> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(invalid(format!("the policy nests deeper than {MAX_DEPTH}")));
        }
        let r = if self.word("not") {
            self.pos += 1;
            self.unary().map(|e| Expr::Not(Box::new(e)))
        } else if self.peek() == Some(&Tok::LParen) {
            self.pos += 1;
            let e = self.or_expr()?;
            if self.peek() != Some(&Tok::RParen) {
                return Err(invalid(format!("expected ) at {}", self.at())));
            }
            self.pos += 1;
            Ok(e)
        } else {
            self.pred()
        };
        self.depth -= 1;
        r
    }

    fn string(&mut self) -> Result<String> {
        match self.peek() {
            Some(Tok::Str(s)) => {
                let s = s.clone();
                self.pos += 1;
                Ok(s)
            }
            _ => Err(invalid(format!(
                "expected a double-quoted string at {}",
                self.at()
            ))),
        }
    }

    fn pred(&mut self) -> Result<Expr> {
        let name = match self.peek() {
            Some(Tok::Word(w)) if !keyword(w) => {
                if w != "dc" && !valid_tag_key(w) {
                    return Err(invalid(format!(
                        "{w:?} at {} isn't a tag key: [a-z][a-z0-9_]{{0,31}}",
                        self.at()
                    )));
                }
                w.clone()
            }
            _ => {
                return Err(invalid(format!(
                    "expected a tag name, not, or ( at {}",
                    self.at()
                )));
            }
        };
        self.pos += 1;
        match self.peek() {
            Some(Tok::Eq) => {
                self.pos += 1;
                Ok(Expr::Eq(name, self.string()?))
            }
            Some(Tok::Ne) => {
                self.pos += 1;
                Ok(Expr::Ne(name, self.string()?))
            }
            Some(Tok::Word(w)) if w == "in" => {
                self.pos += 1;
                if self.peek() != Some(&Tok::LParen) {
                    return Err(invalid(format!("expected ( after in at {}", self.at())));
                }
                self.pos += 1;
                let mut list = vec![self.string()?];
                while self.peek() == Some(&Tok::Comma) {
                    self.pos += 1;
                    list.push(self.string()?);
                }
                if self.peek() != Some(&Tok::RParen) {
                    return Err(invalid(format!("expected , or ) at {}", self.at())));
                }
                self.pos += 1;
                Ok(Expr::In(name, list))
            }
            _ => Err(invalid(format!(
                "expected =, != or in after {name:?} at {}",
                self.at()
            ))),
        }
    }
}

fn tag<'a>(dc: &'a DcRecord, name: &str) -> Option<std::borrow::Cow<'a, str>> {
    if name == "dc" {
        return Some(std::borrow::Cow::Borrowed(dc.dc_id.as_str()));
    }
    match dc.tags.get(name)? {
        Value::String(s) => Some(std::borrow::Cow::Borrowed(s.as_str())),
        Value::Null => None,
        v => Some(std::borrow::Cow::Owned(v.to_string())),
    }
}

impl Expr {
    fn eval(&self, dc: &DcRecord) -> bool {
        match self {
            Expr::Or(v) => v.iter().any(|e| e.eval(dc)),
            Expr::And(v) => v.iter().all(|e| e.eval(dc)),
            Expr::Not(e) => !e.eval(dc),
            Expr::Eq(n, s) => tag(dc, n).is_some_and(|t| t == s.as_str()),
            Expr::Ne(n, s) => tag(dc, n).is_none_or(|t| t != s.as_str()),
            Expr::In(n, l) => tag(dc, n).is_some_and(|t| l.iter().any(|s| s == t.as_ref())),
        }
    }

    fn names(&self, out: &mut BTreeSet<String>) {
        match self {
            Expr::Or(v) | Expr::And(v) => v.iter().for_each(|e| e.names(out)),
            Expr::Not(e) => e.names(out),
            Expr::Eq(n, _) | Expr::Ne(n, _) | Expr::In(n, _) => {
                out.insert(n.clone());
            }
        }
    }
}

impl Policy {
    /// Parses `text`; blank text is the empty policy.
    ///
    /// # Errors
    /// 39506 `ER_SPRIG_RESIDENCY_INVALID`, naming where it went wrong.
    pub fn parse(text: &str) -> Result<Policy> {
        if text.len() > MAX_POLICY_LEN {
            return Err(invalid(format!(
                "the policy is longer than {MAX_POLICY_LEN} bytes"
            )));
        }
        let toks = lex(text)?;
        if toks.is_empty() {
            return Ok(Policy::default());
        }
        let mut p = Parser {
            toks,
            pos: 0,
            depth: 0,
        };
        let ast = p.or_expr()?;
        if p.pos != p.toks.len() {
            return Err(invalid(format!("unexpected text at {}", p.at())));
        }
        Ok(Policy {
            text: text.trim().to_owned(),
            ast: Some(ast),
        })
    }

    /// The text it was parsed from, trimmed.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Whether this is the empty policy (whatever the parent allows).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ast.is_none()
    }

    /// Whether the policy admits `dc`.
    #[must_use]
    pub fn admits(&self, dc: &DcRecord) -> bool {
        self.ast.as_ref().is_none_or(|e| e.eval(dc))
    }

    /// The tag names the policy reads (`dc` included).
    #[must_use]
    pub fn names(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        if let Some(e) = &self.ast {
            e.names(&mut out);
        }
        out
    }

    /// A short, stable hash of the text (FNV-1a, 64 bits, hex), for the
    /// directory block's `policy_hash`.
    #[must_use]
    pub fn hash(&self) -> String {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in self.text.bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        format!("{h:016x}")
    }

    /// DCs the policy admits only because a tag it names is missing
    /// (through `!=` or `not`): step 3's warning.
    #[must_use]
    pub fn admitted_without_tags<'a>(&self, registry: &'a [DcRecord]) -> Vec<(&'a DcId, String)> {
        let names = self.names();
        registry
            .iter()
            .filter(|d| eligible_dc(d) && self.admits(d))
            .filter_map(|d| {
                let missing: Vec<&str> = names
                    .iter()
                    .filter(|n| n.as_str() != "dc" && tag(d, n).is_none())
                    .map(String::as_str)
                    .collect();
                (!missing.is_empty()).then(|| (&d.dc_id, missing.join(", ")))
            })
            .collect()
    }
}

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.text.is_empty() {
            f.write_str("(empty: every DC)")
        } else {
            f.write_str(&self.text)
        }
    }
}

/// A DC a tenant may be placed in at all: `full` and not retired.
#[must_use]
pub fn eligible_dc(dc: &DcRecord) -> bool {
    dc.kind == "full" && dc.state != "retired"
}

/// `resolve(p) = sorted { dc | dc.kind = full, dc.state != retired,
/// admits(p, dc) }`.
#[must_use]
pub fn resolve(p: &Policy, registry: &[DcRecord]) -> BTreeSet<DcId> {
    registry
        .iter()
        .filter(|d| eligible_dc(d) && p.admits(d))
        .map(|d| d.dc_id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::needless_raw_string_hashes)]

    use serde_json::{Map, json};

    use super::*;

    fn dc(id: &str, tags: Value) -> DcRecord {
        let tags: Map<String, Value> = match tags {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        DcRecord {
            dc_id: DcId::new(id).expect("dc"),
            kind: "full".into(),
            state: "active".into(),
            dc_epoch: 1,
            tags,
            global_voter: false,
        }
    }

    fn registry() -> Vec<DcRecord> {
        let mut wit = dc("wit1", json!({"jurisdiction": "EU"}));
        wit.kind = "witness".into();
        let mut old = dc("par1", json!({"jurisdiction": "EU", "country": "FR"}));
        old.state = "retired".into();
        vec![
            dc(
                "fra1",
                json!({"jurisdiction": "EU", "country": "DE", "provider": "acme-cloud"}),
            ),
            dc("ams1", json!({"jurisdiction": "EU", "country": "NL"})),
            dc("mum1", json!({"jurisdiction": "IN", "country": "IN"})),
            dc("iad1", json!({"country": "US", "tier": 2})),
            wit,
            old,
        ]
    }

    fn ids(p: &str) -> Vec<String> {
        resolve(&Policy::parse(p).expect(p), &registry())
            .into_iter()
            .map(|d| d.to_string())
            .collect()
    }

    #[test]
    fn evaluates_the_books_examples() {
        assert_eq!(ids(r#"jurisdiction = "EU""#), ["ams1", "fra1"]);
        assert_eq!(
            ids(r#"country in ("DE", "FR") and provider != "acme-cloud""#),
            Vec::<String>::new()
        );
        assert_eq!(ids(r#"country in ("DE", "NL")"#), ["ams1", "fra1"]);
        assert_eq!(ids(r#"dc in ("fra1", "ams1")"#), ["ams1", "fra1"]);
        // Empty: every full, unretired DC.
        assert_eq!(ids("  "), ["ams1", "fra1", "iad1", "mum1"]);
    }

    #[test]
    fn missing_tags_not_and_precedence() {
        // != holds for a missing tag; = and in don't.
        assert_eq!(
            ids(r#"jurisdiction != "EU""#),
            ["iad1", "mum1"],
            "iad1 has no jurisdiction"
        );
        assert_eq!(ids(r#"not jurisdiction = "EU""#), ["iad1", "mum1"]);
        assert_eq!(ids(r#"jurisdiction in ("US")"#), Vec::<String>::new());
        // and binds tighter than or.
        assert_eq!(
            ids(r#"dc = "mum1" or jurisdiction = "EU" and country = "NL""#),
            ["ams1", "mum1"]
        );
        assert_eq!(
            ids(r#"(dc = "mum1" or jurisdiction = "EU") and country = "NL""#),
            ["ams1"]
        );
        // Non-string tags compare by their JSON text.
        assert_eq!(ids(r#"tier = "2""#), ["iad1"]);
        assert_eq!(ids(r#"not not dc = "fra1""#), ["fra1"]);
        // Escapes.
        let p = Policy::parse(r#"x = "a\"b\\c""#).expect("escapes");
        assert!(p.admits(&dc("t1", json!({"x": "a\"b\\c"}))));
    }

    #[test]
    fn warns_about_dcs_admitted_without_a_named_tag() {
        let p = Policy::parse(r#"jurisdiction != "IN""#).expect("p");
        let reg = registry();
        let w = p.admitted_without_tags(&reg);
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].0.as_str(), "iad1");
        assert_eq!(w[0].1, "jurisdiction");
    }

    #[test]
    fn rejects_bad_policies_with_39506() {
        for bad in [
            r#"jurisdiction = EU"#,
            r#"jurisdiction == "EU""#,
            r#"Jurisdiction = "EU""#,
            r#"jurisdiction = "EU" and"#,
            r#"(jurisdiction = "EU""#,
            r#"jurisdiction in ("EU",)"#,
            r#"jurisdiction in "EU""#,
            r#"x = "unterminated"#,
            r#"x = "bad \n escape""#,
            r#"and = "x""#,
            r#"x = "a" y = "b""#,
            r#"a_very_long_tag_name_over_32_chars = "x""#,
            r#"x ~ "a""#,
        ] {
            let e = Policy::parse(bad).expect_err(bad);
            assert_eq!(e.code(), Some(SprigCode::ResidencyInvalid), "{bad}: {e}");
        }
        let deep = format!("{}x = \"a\"{}", "(".repeat(40), ")".repeat(40));
        assert!(Policy::parse(&deep).is_err());
        assert!(Policy::parse(&"x".repeat(MAX_POLICY_LEN + 1)).is_err());
    }

    #[test]
    fn hash_and_names() {
        let p = Policy::parse(r#"dc = "fra1" or country in ("DE")"#).expect("p");
        assert_eq!(p.names().into_iter().collect::<Vec<_>>(), ["country", "dc"]);
        assert_eq!(p.hash().len(), 16);
        assert_eq!(p.hash(), Policy::parse(p.text()).expect("again").hash());
        assert_ne!(p.hash(), Policy::parse(r#"dc = "fra1""#).expect("o").hash());
    }
}
