//! KIRun's expression language (KIRun's `expressions.md`): a lexer and a
//! precedence-climbing parser with KIRun's operator priorities.
//!
//! Priorities, tightest first: unary `+ - not ~`, `.` and `[`; `* / // %`
//! and `..`; `+ -`; `<< >> >>>`; `< <= > >=`; `= !=`; `&`; `^`; `|`;
//! `and`; `or` and `??`; `? :`. Binary operators associate to the left,
//! the ternary to the right. `{{ e }}` (nested evaluation) parses as a
//! parenthesized `e`.

use std::fmt;

/// A number literal: whole numbers keep their integer value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Num {
    /// A whole number that fits `i64`.
    Int(i64),
    /// Anything else.
    Float(f64),
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KUn {
    /// `+`.
    Plus,
    /// `-`.
    Minus,
    /// `not`.
    Not,
    /// `~`.
    BitNot,
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum KBin {
    Mul,
    Div,
    IntDiv,
    Mod,
    Add,
    Sub,
    Shl,
    Shr,
    UShr,
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    BitAnd,
    BitXor,
    BitOr,
    And,
    Or,
    Coalesce,
}

impl KBin {
    /// KIRun's priority: lower binds tighter.
    const fn priority(self) -> u8 {
        match self {
            KBin::Mul | KBin::Div | KBin::IntDiv | KBin::Mod => 2,
            KBin::Add | KBin::Sub => 3,
            KBin::Shl | KBin::Shr | KBin::UShr => 4,
            KBin::Lt | KBin::Le | KBin::Gt | KBin::Ge => 5,
            KBin::Eq | KBin::Ne => 6,
            KBin::BitAnd => 7,
            KBin::BitXor => 8,
            KBin::BitOr => 9,
            KBin::And => 10,
            KBin::Or | KBin::Coalesce => 11,
        }
    }

    /// The operator's text.
    #[must_use]
    pub const fn text(self) -> &'static str {
        match self {
            KBin::Mul => "*",
            KBin::Div => "/",
            KBin::IntDiv => "//",
            KBin::Mod => "%",
            KBin::Add => "+",
            KBin::Sub => "-",
            KBin::Shl => "<<",
            KBin::Shr => ">>",
            KBin::UShr => ">>>",
            KBin::Lt => "<",
            KBin::Le => "<=",
            KBin::Gt => ">",
            KBin::Ge => ">=",
            KBin::Eq => "=",
            KBin::Ne => "!=",
            KBin::BitAnd => "&",
            KBin::BitXor => "^",
            KBin::BitOr => "|",
            KBin::And => "and",
            KBin::Or => "or",
            KBin::Coalesce => "??",
        }
    }
}

/// A parsed expression.
#[derive(Debug, Clone, PartialEq)]
pub enum KExpr {
    /// A number.
    Num(Num),
    /// A string.
    Str(String),
    /// `true` or `false`.
    Bool(bool),
    /// `null` or `undefined`.
    Null,
    /// A bare name: `Arguments`, `Steps`, `Context`, ...
    Ident(String),
    /// `e.name`.
    Member(Box<KExpr>, String),
    /// `e[i]`.
    Index(Box<KExpr>, Box<KExpr>),
    /// `e[a..b]`.
    Range(Box<KExpr>, Option<Box<KExpr>>, Option<Box<KExpr>>),
    /// A unary operator.
    Unary(KUn, Box<KExpr>),
    /// A binary operator.
    Binary(KBin, Box<KExpr>, Box<KExpr>),
    /// `c ? a : b`.
    Ternary(Box<KExpr>, Box<KExpr>, Box<KExpr>),
}

impl KExpr {
    /// The chain of member names when the expression is a plain path
    /// (`Steps.item.output.row`), root first.
    #[must_use]
    pub fn path(&self) -> Option<Vec<&str>> {
        match self {
            KExpr::Ident(n) => Some(vec![n.as_str()]),
            KExpr::Member(b, n) => {
                let mut p = b.path()?;
                p.push(n.as_str());
                Some(p)
            }
            _ => None,
        }
    }

    /// Every `Steps.<step>.<event>` this expression reads.
    #[must_use]
    pub fn step_refs(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        self.collect_steps(&mut out);
        out
    }

    fn collect_steps(&self, out: &mut Vec<(String, String)>) {
        // The innermost Member chain rooted at `Steps` with two members.
        if let KExpr::Member(b, ev) = self
            && let KExpr::Member(r, step) = b.as_ref()
            && matches!(r.as_ref(), KExpr::Ident(s) if s == "Steps")
        {
            out.push((step.clone(), ev.clone()));
            return;
        }
        match self {
            KExpr::Num(_) | KExpr::Str(_) | KExpr::Bool(_) | KExpr::Null | KExpr::Ident(_) => {}
            KExpr::Member(b, _) | KExpr::Unary(_, b) => b.collect_steps(out),
            KExpr::Index(a, b) | KExpr::Binary(_, a, b) => {
                a.collect_steps(out);
                b.collect_steps(out);
            }
            KExpr::Range(a, b, c) => {
                a.collect_steps(out);
                for x in [b, c].into_iter().flatten() {
                    x.collect_steps(out);
                }
            }
            KExpr::Ternary(a, b, c) => {
                a.collect_steps(out);
                b.collect_steps(out);
                c.collect_steps(out);
            }
        }
    }
}

/// A parse error, with the byte offset where it was found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    /// Byte offset into the expression.
    pub at: usize,
    /// What was wrong.
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at offset {}", self.message, self.at)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(Num),
    Str(String),
    Word(String),
    Op(&'static str),
}

const OPS: &[&str] = &[
    ">>>", "{{", "}}", "//", "<<", ">>", "<=", ">=", "!=", "??", "..", "*", "/", "%", "+", "-",
    "<", ">", "=", "&", "|", "^", "~", "?", ":", ".", "[", "]", "(", ")",
];

fn lex(src: &str) -> Result<Vec<(usize, Tok)>, ParseError> {
    let b = src.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        if c.is_ascii_digit() {
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            let mut is_float = false;
            // A fraction, but not the range operator `..`.
            if i + 1 < b.len() && b[i] == b'.' && b[i + 1].is_ascii_digit() {
                is_float = true;
                i += 1;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
                let mut j = i + 1;
                if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
                    j += 1;
                }
                if j < b.len() && b[j].is_ascii_digit() {
                    is_float = true;
                    i = j;
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                }
            }
            let text = &src[start..i];
            let n = if is_float {
                Num::Float(text.parse().map_err(|_| err(start, "bad number"))?)
            } else {
                match text.parse::<i64>() {
                    Ok(v) => Num::Int(v),
                    Err(_) => Num::Float(text.parse().map_err(|_| err(start, "bad number"))?),
                }
            };
            out.push((start, Tok::Num(n)));
            continue;
        }
        if c == b'\'' || c == b'"' {
            let q = c;
            i += 1;
            let mut s = String::new();
            loop {
                let Some(ch) = src[i..].chars().next() else {
                    return Err(err(start, "unterminated string"));
                };
                i += ch.len_utf8();
                if ch as u32 == u32::from(q) {
                    break;
                }
                if ch == '\\' {
                    let Some(e) = src[i..].chars().next() else {
                        return Err(err(start, "unterminated string"));
                    };
                    i += e.len_utf8();
                    s.push(match e {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        other => other,
                    });
                } else {
                    s.push(ch);
                }
            }
            out.push((start, Tok::Str(s)));
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' || c == b'$' {
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] == b'-')
            {
                // KIRun step names may hold '-'; a '-' followed by a space or
                // a digit after a space is subtraction, so only take '-' when
                // a letter follows directly.
                if b[i] == b'-' && !(i + 1 < b.len() && b[i + 1].is_ascii_alphabetic()) {
                    break;
                }
                i += 1;
            }
            out.push((start, Tok::Word(src[start..i].to_owned())));
            continue;
        }
        let Some(op) = OPS.iter().find(|o| src[i..].starts_with(**o)) else {
            return Err(err(start, &format!("unexpected character {:?}", c as char)));
        };
        i += op.len();
        out.push((start, Tok::Op(op)));
    }
    Ok(out)
}

fn err(at: usize, m: &str) -> ParseError {
    ParseError {
        at,
        message: m.to_owned(),
    }
}

/// Parses one expression.
///
/// # Errors
/// A [`ParseError`] for anything outside the grammar.
pub fn parse(src: &str) -> Result<KExpr, ParseError> {
    let toks = lex(src)?;
    let mut p = Parser {
        toks,
        pos: 0,
        len: src.len(),
    };
    let e = p.ternary()?;
    if p.pos < p.toks.len() {
        return Err(err(p.at(), "unexpected token"));
    }
    Ok(e)
}

struct Parser {
    toks: Vec<(usize, Tok)>,
    pos: usize,
    len: usize,
}

impl Parser {
    fn at(&self) -> usize {
        self.toks.get(self.pos).map_or(self.len, |t| t.0)
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.1)
    }

    fn eat_op(&mut self, op: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Op(o)) if *o == op) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, op: &str) -> Result<(), ParseError> {
        if self.eat_op(op) {
            Ok(())
        } else {
            Err(err(self.at(), &format!("expected {op}")))
        }
    }

    fn ternary(&mut self) -> Result<KExpr, ParseError> {
        let c = self.binary(11)?;
        if self.eat_op("?") {
            let a = self.ternary()?;
            self.expect(":")?;
            let b = self.ternary()?;
            return Ok(KExpr::Ternary(Box::new(c), Box::new(a), Box::new(b)));
        }
        Ok(c)
    }

    fn bin_op(&self) -> Option<KBin> {
        Some(match self.peek()? {
            Tok::Op(o) => match *o {
                "*" => KBin::Mul,
                "/" => KBin::Div,
                "//" => KBin::IntDiv,
                "%" => KBin::Mod,
                "+" => KBin::Add,
                "-" => KBin::Sub,
                "<<" => KBin::Shl,
                ">>" => KBin::Shr,
                ">>>" => KBin::UShr,
                "<" => KBin::Lt,
                "<=" => KBin::Le,
                ">" => KBin::Gt,
                ">=" => KBin::Ge,
                "=" => KBin::Eq,
                "!=" => KBin::Ne,
                "&" => KBin::BitAnd,
                "^" => KBin::BitXor,
                "|" => KBin::BitOr,
                "??" => KBin::Coalesce,
                _ => return None,
            },
            Tok::Word(w) => match w.as_str() {
                "and" => KBin::And,
                "or" => KBin::Or,
                _ => return None,
            },
            _ => return None,
        })
    }

    /// Operators of priority `max` or tighter.
    fn binary(&mut self, max: u8) -> Result<KExpr, ParseError> {
        if max < 2 {
            return self.unary();
        }
        let mut left = self.binary(max - 1)?;
        while let Some(op) = self.bin_op() {
            if op.priority() != max {
                break;
            }
            self.pos += 1;
            let right = self.binary(max - 1)?;
            left = KExpr::Binary(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<KExpr, ParseError> {
        let op = match self.peek() {
            Some(Tok::Op("+")) => Some(KUn::Plus),
            Some(Tok::Op("-")) => Some(KUn::Minus),
            Some(Tok::Op("~")) => Some(KUn::BitNot),
            Some(Tok::Word(w)) if w == "not" => Some(KUn::Not),
            _ => None,
        };
        if let Some(op) = op {
            self.pos += 1;
            let e = self.unary()?;
            // Fold `-<number>` into a literal.
            return Ok(match (op, e) {
                (KUn::Minus, KExpr::Num(Num::Int(v))) => KExpr::Num(Num::Int(-v)),
                (KUn::Minus, KExpr::Num(Num::Float(v))) => KExpr::Num(Num::Float(-v)),
                (KUn::Plus, e @ KExpr::Num(_)) => e,
                (op, e) => KExpr::Unary(op, Box::new(e)),
            });
        }
        self.postfix()
    }

    fn postfix(&mut self) -> Result<KExpr, ParseError> {
        let mut e = self.primary()?;
        loop {
            if self.eat_op(".") {
                match self.toks.get(self.pos).map(|t| t.1.clone()) {
                    Some(Tok::Word(w)) => {
                        self.pos += 1;
                        e = KExpr::Member(Box::new(e), w);
                    }
                    Some(Tok::Num(Num::Int(n))) => {
                        self.pos += 1;
                        e = KExpr::Member(Box::new(e), n.to_string());
                    }
                    _ => return Err(err(self.at(), "expected a name after '.'")),
                }
            } else if self.eat_op("[") {
                if self.eat_op("..") {
                    let hi = if self.eat_op("]") {
                        None
                    } else {
                        let h = self.ternary()?;
                        self.expect("]")?;
                        Some(Box::new(h))
                    };
                    e = KExpr::Range(Box::new(e), None, hi);
                    continue;
                }
                let i = self.ternary()?;
                if self.eat_op("..") {
                    let hi = if self.eat_op("]") {
                        None
                    } else {
                        let h = self.ternary()?;
                        self.expect("]")?;
                        Some(Box::new(h))
                    };
                    e = KExpr::Range(Box::new(e), Some(Box::new(i)), hi);
                    continue;
                }
                self.expect("]")?;
                e = KExpr::Index(Box::new(e), Box::new(i));
            } else {
                return Ok(e);
            }
        }
    }

    fn primary(&mut self) -> Result<KExpr, ParseError> {
        let at = self.at();
        let Some((_, t)) = self.toks.get(self.pos).cloned() else {
            return Err(err(at, "unexpected end of expression"));
        };
        self.pos += 1;
        Ok(match t {
            Tok::Num(n) => KExpr::Num(n),
            Tok::Str(s) => KExpr::Str(s),
            Tok::Word(w) => match w.as_str() {
                "true" => KExpr::Bool(true),
                "false" => KExpr::Bool(false),
                "null" | "undefined" => KExpr::Null,
                _ => KExpr::Ident(w),
            },
            Tok::Op("(") => {
                let e = self.ternary()?;
                self.expect(")")?;
                e
            }
            Tok::Op("{{") => {
                let e = self.ternary()?;
                self.expect("}}")?;
                e
            }
            Tok::Op(o) => return Err(err(at, &format!("unexpected {o}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> KExpr {
        parse(s).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    fn show(e: &KExpr) -> String {
        match e {
            KExpr::Num(Num::Int(i)) => i.to_string(),
            KExpr::Num(Num::Float(f)) => format!("{f:?}"),
            KExpr::Str(s) => format!("{s:?}"),
            KExpr::Bool(b) => b.to_string(),
            KExpr::Null => "null".into(),
            KExpr::Ident(n) => n.clone(),
            KExpr::Member(b, n) => format!("{}.{n}", show(b)),
            KExpr::Index(b, i) => format!("{}[{}]", show(b), show(i)),
            KExpr::Range(b, lo, hi) => format!(
                "{}[{}..{}]",
                show(b),
                lo.as_deref().map(show).unwrap_or_default(),
                hi.as_deref().map(show).unwrap_or_default()
            ),
            KExpr::Unary(op, e) => format!("({op:?} {})", show(e)),
            KExpr::Binary(op, a, b) => format!("({} {} {})", show(a), op.text(), show(b)),
            KExpr::Ternary(c, a, b) => format!("({} ? {} : {})", show(c), show(a), show(b)),
        }
    }

    #[test]
    fn precedence_follows_kirun() {
        assert_eq!(show(&p("1 + 2 * 3")), "(1 + (2 * 3))");
        assert_eq!(
            show(&p("a.b + 1 > 2 and c = 'x' or d")),
            "((((a.b + 1) > 2) and (c = \"x\")) or d)"
        );
        assert_eq!(show(&p("not a = b")), "((Not a) = b)");
        assert_eq!(show(&p("a ?? b ?? 'z'")), "((a ?? b) ?? \"z\")");
        assert_eq!(
            show(&p("x >= 18 ? 'adult' : 'minor'")),
            "((x >= 18) ? \"adult\" : \"minor\")"
        );
        assert_eq!(show(&p("a - b - c")), "((a - b) - c)");
        assert_eq!(show(&p("7 // 2 % 3")), "((7 // 2) % 3)");
        assert_eq!(show(&p("-3 * -x")), "(-3 * (Minus x))");
        assert_eq!(show(&p("(a + b) * c")), "((a + b) * c)");
        assert_eq!(show(&p("1 << 2 + 1")), "(1 << (2 + 1))");
        assert_eq!(show(&p("a & b | c ^ d")), "((a & b) | (c ^ d))");
    }

    #[test]
    fn paths_indexes_and_ranges() {
        assert_eq!(
            show(&p("Steps.item.output.row.price * Arguments.qty")),
            "(Steps.item.output.row.price * Arguments.qty)"
        );
        assert_eq!(
            show(&p("Arguments.items[0].name")),
            "Arguments.items[0].name"
        );
        assert_eq!(
            show(&p("Arguments.cfg[\"mail.host\"]")),
            "Arguments.cfg[\"mail.host\"]"
        );
        assert_eq!(show(&p("Arguments.items[2..5]")), "Arguments.items[2..5]");
        assert_eq!(show(&p("Arguments.items[..3]")), "Arguments.items[..3]");
        assert_eq!(show(&p("Arguments.items[3..]")), "Arguments.items[3..]");
        assert_eq!(
            show(&p("Steps.items[{{Arguments.index}}].name")),
            "Steps.items[Arguments.index].name"
        );
        assert_eq!(show(&p("2.5e1 + 1.5")), "(25.0 + 1.5)");
    }

    #[test]
    fn step_references() {
        let e = p("Steps.item.output.row.stock >= Arguments.qty and Steps.x-y.true");
        assert_eq!(
            e.step_refs(),
            vec![
                ("item".into(), "output".into()),
                ("x-y".into(), "true".into())
            ]
        );
        assert_eq!(
            p("Steps.a.output").path(),
            Some(vec!["Steps", "a", "output"])
        );
    }

    #[test]
    fn errors_have_offsets() {
        let e = parse("1 + ").unwrap_err();
        assert_eq!(e.at, 4);
        assert!(parse("'abc").is_err());
        assert!(parse("a ? b").is_err());
        assert!(parse("a b").is_err());
        assert!(parse("a # b").is_err());
    }
}
