//! The statements and clauses of inherited messaging objects (chapter 42,
//! "Syntax"; chapter 45, "Inherited streams"), recognized on chapter 07's
//! lexer:
//!
//! ```text
//! CREATE ... name FOR EACH TENANT [CHILDREN | DESCENDANTS] [UNDER t]
//!   [INCLUDING SELF] [GRANTS (priv [, priv] ON db.obj [, ...])] ...
//! INSERT [IGNORE] [INTO] s FOR TENANT t (...) ...
//! INSERT [IGNORE] [INTO] s FOR ALL TENANTS [WITH INSTANCE] [UNDER t] (...) ...
//! INSERT [IGNORE] [INTO] s IN TENANT t (...) ...       -- into an ancestor's stream
//! ALTER INHERITED kind [db.]name [FOR TENANT t | FOR SUBTREE [t]]
//!   SPRIG SET (k = v, ...) | SET (k = v, ...) | RESET [(k, ...)]
//!   | DISABLE | ENABLE | DETACH | REPLACE LANGUAGE x AS body
//! SHOW INHERITED DEFINITIONS | INSTANCES | OVERRIDES [FOR kind [db.]name]
//! SHOW INHERITED BROADCASTS FOR kind [db.]name
//! ```
//!
//! The kind-specific rest of a `CREATE` is parsed by its own chapter's
//! grammar once the clause is cut out ([`ForEach::range`]).

use std::ops::Range;

use sprig_common::{Result, SprigError};
use sprig_sql::lex::{LexOptions, Token, TokenKind, ident_value, lex, string_value};

use super::model::{Depth, GrantItem};

fn syntax(near: &str) -> SprigError {
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             server version for the right syntax to use near '{near}'"
        ),
    )
}

/// The non-trivia tokens of a statement, with a cursor.
struct Cur<'a> {
    src: &'a str,
    toks: Vec<Token>,
    i: usize,
}

impl<'a> Cur<'a> {
    #[allow(clippy::trivially_copy_pass_by_ref)] // the lexer takes a reference
    fn new(src: &'a str, opts: &LexOptions) -> Result<Self> {
        let toks = lex(src.as_bytes(), opts)
            .map_err(|_| syntax("(statement too long)"))?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        Ok(Self { src, toks, i: 0 })
    }

    fn text(&self, t: &Token) -> &'a str {
        &self.src[t.range()]
    }

    fn peek_at(&self, k: usize) -> Option<&Token> {
        self.toks.get(self.i + k)
    }

    fn word_at(&self, k: usize, w: &str) -> bool {
        self.peek_at(k)
            .is_some_and(|t| t.kind == TokenKind::Word && self.text(t).eq_ignore_ascii_case(w))
    }

    fn eat(&mut self, w: &str) -> bool {
        if self.word_at(0, w) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn punct(&mut self, p: &str) -> bool {
        if self.peek_at(0).is_some_and(|t| {
            matches!(t.kind, TokenKind::Punct | TokenKind::Op) && self.text(t) == p
        }) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn near(&self) -> String {
        self.peek_at(0).map_or_else(String::new, |t| {
            self.src[t.start as usize..].chars().take(40).collect()
        })
    }

    /// An identifier or a string literal (tenant names may be either).
    fn name(&mut self) -> Result<String> {
        let t = *self.peek_at(0).ok_or_else(|| syntax(""))?;
        let v = match t.kind {
            TokenKind::Word | TokenKind::QuotedIdent => {
                String::from_utf8_lossy(&ident_value(t.bytes(self.src.as_bytes()))).into_owned()
            }
            TokenKind::String => string_value(t.bytes(self.src.as_bytes()), false)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .ok_or_else(|| syntax(self.text(&t)))?,
            _ => return Err(syntax(&self.near())),
        };
        self.i += 1;
        Ok(v)
    }

    /// `[db.]name` (`*` allowed as the name when `star`).
    fn qualified(&mut self, star: bool) -> Result<(Option<String>, String)> {
        let first = if star && self.punct("*") {
            "*".to_owned()
        } else {
            self.name()?
        };
        if self.punct(".") {
            let second = if star && self.punct("*") {
                "*".to_owned()
            } else {
                self.name()?
            };
            return Ok((Some(first), second));
        }
        Ok((None, first))
    }

    fn end_of(&self, k: usize) -> usize {
        self.toks.get(k).map_or(self.src.len(), |t| t.end as usize)
    }
}

/// A parsed `FOR EACH TENANT` clause.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ForEach {
    /// `CHILDREN` or `DESCENDANTS`.
    pub depth: Depth,
    /// `UNDER t`.
    pub under: Option<String>,
    /// `INCLUDING SELF`.
    pub including_self: bool,
    /// `GRANTS (...)`.
    pub grants: Vec<GrantItem>,
    /// The clause's bytes, to cut out before the kind's own grammar.
    pub range: Range<usize>,
}

impl ForEach {
    /// `sql` without the clause.
    #[must_use]
    pub fn strip(&self, sql: &str) -> String {
        let mut s = String::with_capacity(sql.len());
        s.push_str(&sql[..self.range.start]);
        s.push(' ');
        s.push_str(&sql[self.range.end..]);
        s
    }
}

/// Finds `FOR EACH TENANT ...` before the statement's body (the first
/// `AS` or `LANGUAGE` word, so a trigger's code is never searched).
///
/// # Errors
/// 1064 for a malformed clause.
pub fn for_each(sql: &str, opts: &LexOptions) -> Result<Option<ForEach>> {
    let mut c = Cur::new(sql, opts)?;
    while c.i < c.toks.len() {
        if c.word_at(0, "as") || c.word_at(0, "language") {
            return Ok(None);
        }
        if c.word_at(0, "for") && c.word_at(1, "each") && c.word_at(2, "tenant") {
            let start = c.toks[c.i].start as usize;
            c.i += 3;
            let mut f = ForEach::default();
            loop {
                if c.eat("children") {
                    f.depth = Depth::Children;
                } else if c.eat("descendants") {
                    f.depth = Depth::Descendants;
                } else if c.eat("under") {
                    f.under = Some(c.name()?);
                } else if c.word_at(0, "including") && c.word_at(1, "self") {
                    c.i += 2;
                    f.including_self = true;
                } else if c.eat("grants") {
                    f.grants = grants(&mut c)?;
                } else if c.word_at(0, "with") && c.word_at(1, "app") {
                    return Err(SprigError::standard(
                        1064,
                        *b"42000",
                        "WITH APP isn't SprigDB syntax: SprigDB has no apps; declare the \
                         definition where its tenants are (FOR EACH TENANT [UNDER t])",
                    ));
                } else {
                    break;
                }
            }
            f.range = start..c.end_of(c.i.saturating_sub(1));
            return Ok(Some(f));
        }
        c.i += 1;
    }
    Ok(None)
}

/// `(priv [, priv] ON db.obj [, ...])`.
fn grants(c: &mut Cur<'_>) -> Result<Vec<GrantItem>> {
    if !c.punct("(") {
        return Err(syntax(&c.near()));
    }
    let mut out = Vec::new();
    let mut privs: Vec<String> = Vec::new();
    loop {
        if c.punct(")") {
            if !privs.is_empty() {
                return Err(syntax("GRANTS: privileges without ON"));
            }
            break;
        }
        if c.eat("on") {
            if privs.is_empty() {
                return Err(syntax("GRANTS: ON without privileges"));
            }
            let (db, name) = c.qualified(true)?;
            let (db, name) = match db {
                Some(d) => (d, name),
                None => {
                    return Err(SprigError::standard(
                        1064,
                        *b"42000",
                        format!("GRANTS: name the database of '{name}' (db.object or db.*)"),
                    ));
                }
            };
            out.push(GrantItem {
                privileges: std::mem::take(&mut privs),
                db,
                name,
            });
            if c.punct(",") {
                continue;
            }
            if c.punct(")") {
                break;
            }
            return Err(syntax(&c.near()));
        }
        // A privilege: one or more words (`ALTER ROUTINE`).
        let mut p = String::new();
        while let Some(t) = c.peek_at(0).copied() {
            if t.kind != TokenKind::Word || c.text(&t).eq_ignore_ascii_case("on") {
                break;
            }
            if !p.is_empty() {
                p.push(' ');
            }
            p.push_str(&c.text(&t).to_ascii_uppercase());
            c.i += 1;
        }
        if p.is_empty() {
            return Err(syntax(&c.near()));
        }
        privs.push(p);
        let _ = c.punct(",");
    }
    Ok(out)
}

/// Where a publish goes, named after the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// `FOR TENANT t`.
    Tenant(String),
    /// `FOR ALL TENANTS [WITH INSTANCE] [UNDER t]`.
    All {
        /// `WITH INSTANCE`.
        with_instance: bool,
        /// `UNDER t`.
        under: Option<String>,
    },
    /// `IN TENANT t`: into an ancestor's ordinary stream.
    In(String),
}

/// The target clause of an `INSERT` and its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertTarget {
    /// The clause.
    pub target: Target,
    /// `[db.]name` the insert names.
    pub db: Option<String>,
    /// The stream's name.
    pub name: String,
    /// The clause's bytes.
    pub range: Range<usize>,
}

impl InsertTarget {
    /// `sql` without the clause.
    #[must_use]
    pub fn strip(&self, sql: &str) -> String {
        let mut s = String::with_capacity(sql.len());
        s.push_str(&sql[..self.range.start]);
        s.push(' ');
        s.push_str(&sql[self.range.end..]);
        s
    }
}

/// `INSERT [modifiers] [INTO] [db.]s FOR TENANT t | FOR ALL TENANTS ... |
/// IN TENANT t`; `None` for any other statement.
///
/// # Errors
/// 1064 for a malformed clause.
pub fn insert_target(sql: &str, opts: &LexOptions) -> Result<Option<InsertTarget>> {
    let mut c = Cur::new(sql, opts)?;
    if !c.eat("insert") {
        return Ok(None);
    }
    while c.eat("low_priority") || c.eat("delayed") || c.eat("high_priority") || c.eat("ignore") {}
    let _ = c.eat("into");
    if !c.peek_at(0).is_some_and(|t| t.kind.is_ident_like()) {
        return Ok(None);
    }
    let (db, name) = c.qualified(false)?;
    let Some(start_tok) = c.peek_at(0).copied() else {
        return Ok(None);
    };
    let start = start_tok.start as usize;
    let target = if c.word_at(0, "for") && c.word_at(1, "tenant") {
        c.i += 2;
        Target::Tenant(c.name()?)
    } else if c.word_at(0, "for") && c.word_at(1, "all") && c.word_at(2, "tenants") {
        c.i += 3;
        let mut with_instance = false;
        let mut under = None;
        loop {
            if c.word_at(0, "with") && c.word_at(1, "instance") {
                c.i += 2;
                with_instance = true;
            } else if c.eat("under") {
                under = Some(c.name()?);
            } else {
                break;
            }
        }
        Target::All {
            with_instance,
            under,
        }
    } else if c.word_at(0, "in") && c.word_at(1, "tenant") {
        c.i += 2;
        Target::In(c.name()?)
    } else {
        return Ok(None);
    };
    Ok(Some(InsertTarget {
        target,
        db,
        name,
        range: start..c.end_of(c.i - 1),
    }))
}

/// Which tenants an override is for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum For {
    /// The session's own tenant (no clause).
    #[default]
    Own,
    /// `FOR TENANT t`: `t` alone.
    Tenant(String),
    /// `FOR SUBTREE [t]`: `t` (the session's tenant by default) and its
    /// descendants.
    Subtree(Option<String>),
}

/// What `ALTER INHERITED` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterOp {
    /// `[SPRIG] SET (k = v, ...)`: settings (values as written; strings
    /// unquoted).
    Set(Vec<(String, String)>),
    /// `RESET [(k, ...)]`: every override (none listed) or those settings.
    Reset(Vec<String>),
    /// `DISABLE`.
    Disable,
    /// `ENABLE`.
    Enable,
    /// `DETACH`.
    Detach,
    /// `REPLACE LANGUAGE x AS body`: the text from `LANGUAGE` on.
    Replace(String),
}

/// `ALTER INHERITED ...`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterInherited {
    /// The kind.
    pub kind: super::model::DefKind,
    /// `db.`
    pub db: Option<String>,
    /// The name.
    pub name: String,
    /// Who for.
    pub target: For,
    /// What.
    pub op: AlterOp,
}

/// `SHOW INHERITED ...`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShowInherited {
    /// `SHOW INHERITED DEFINITIONS`: this tenant's definitions.
    Definitions,
    /// `SHOW INHERITED INSTANCES`: this tenant's instances, and the
    /// definitions reaching it.
    Instances,
    /// `SHOW INHERITED OVERRIDES [FOR kind [db.]name]`.
    Overrides(Option<(super::model::DefKind, Option<String>, String)>),
    /// `SHOW INHERITED BROADCASTS FOR kind [db.]name`: a definition's
    /// broadcast log.
    Broadcasts(super::model::DefKind, Option<String>, String),
}

/// The statements of this module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
    /// `ALTER INHERITED ...`.
    Alter(AlterInherited),
    /// `SHOW INHERITED ...`.
    Show(ShowInherited),
}

fn kind_word(c: &mut Cur<'_>) -> Result<super::model::DefKind> {
    let t = *c.peek_at(0).ok_or_else(|| syntax(""))?;
    let k = super::model::DefKind::parse(c.text(&t)).ok_or_else(|| syntax(&c.near()))?;
    c.i += 1;
    Ok(k)
}

fn settings_list(c: &mut Cur<'_>) -> Result<Vec<(String, String)>> {
    if !c.punct("(") {
        return Err(syntax(&c.near()));
    }
    let mut out = Vec::new();
    loop {
        // Keys may be dotted (`stream.retention_hours`).
        let mut k = c.name()?;
        while c.punct(".") {
            k.push('.');
            k.push_str(&c.name()?);
        }
        if !c.punct("=") {
            return Err(syntax(&c.near()));
        }
        let t = *c.peek_at(0).ok_or_else(|| syntax(""))?;
        let v = match t.kind {
            TokenKind::String => string_value(t.bytes(c.src.as_bytes()), false)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .ok_or_else(|| syntax(c.text(&t)))?,
            _ => c.text(&t).to_owned(),
        };
        c.i += 1;
        out.push((k, v));
        if c.punct(",") {
            continue;
        }
        if c.punct(")") {
            break;
        }
        return Err(syntax(&c.near()));
    }
    Ok(out)
}

/// `ALTER INHERITED` or `SHOW INHERITED`; `None` for any other statement.
///
/// # Errors
/// 1064 for a malformed statement.
pub fn statement(sql: &str, opts: &LexOptions) -> Result<Option<Stmt>> {
    let mut c = Cur::new(sql, opts)?;
    if c.word_at(0, "show") && c.word_at(1, "inherited") {
        c.i += 2;
        let s = if c.eat("definitions") {
            ShowInherited::Definitions
        } else if c.eat("instances") {
            ShowInherited::Instances
        } else if c.eat("broadcasts") {
            if !c.eat("for") {
                return Err(syntax(&c.near()));
            }
            let k = kind_word(&mut c)?;
            let (db, name) = c.qualified(false)?;
            ShowInherited::Broadcasts(k, db, name)
        } else if c.eat("overrides") {
            if c.eat("for") {
                let k = kind_word(&mut c)?;
                let (db, name) = c.qualified(false)?;
                ShowInherited::Overrides(Some((k, db, name)))
            } else {
                ShowInherited::Overrides(None)
            }
        } else {
            return Err(syntax(&c.near()));
        };
        return Ok(Some(Stmt::Show(s)));
    }
    if !(c.word_at(0, "alter") && c.word_at(1, "inherited")) {
        return Ok(None);
    }
    c.i += 2;
    let kind = kind_word(&mut c)?;
    let (db, name) = c.qualified(false)?;
    let mut target = For::Own;
    if c.word_at(0, "for") && c.word_at(1, "tenant") {
        c.i += 2;
        target = For::Tenant(c.name()?);
    } else if c.word_at(0, "for") && c.word_at(1, "subtree") {
        c.i += 2;
        let named = c.peek_at(0).is_some_and(|t| {
            matches!(
                t.kind,
                TokenKind::Word | TokenKind::QuotedIdent | TokenKind::String
            )
        }) && ![
            "set", "sprig", "reset", "disable", "enable", "detach", "replace",
        ]
        .iter()
        .any(|w| c.word_at(0, w));
        target = For::Subtree(if named { Some(c.name()?) } else { None });
    }
    let op = if c.eat("sprig") {
        if !c.eat("set") {
            return Err(syntax(&c.near()));
        }
        AlterOp::Set(settings_list(&mut c)?)
    } else if c.eat("set") {
        AlterOp::Set(settings_list(&mut c)?)
    } else if c.eat("reset") {
        let mut keys = Vec::new();
        if c.punct("(") {
            loop {
                let mut k = c.name()?;
                while c.punct(".") {
                    k.push('.');
                    k.push_str(&c.name()?);
                }
                keys.push(k);
                if c.punct(",") {
                    continue;
                }
                if c.punct(")") {
                    break;
                }
                return Err(syntax(&c.near()));
            }
        }
        AlterOp::Reset(keys)
    } else if c.eat("disable") {
        AlterOp::Disable
    } else if c.eat("enable") {
        AlterOp::Enable
    } else if c.eat("detach") {
        AlterOp::Detach
    } else if c.eat("replace") {
        let t = *c.peek_at(0).ok_or_else(|| syntax(""))?;
        if !c.word_at(0, "language") {
            return Err(syntax(&c.near()));
        }
        return Ok(Some(Stmt::Alter(AlterInherited {
            kind,
            db,
            name,
            target,
            op: AlterOp::Replace(
                sql[t.start as usize..]
                    .trim()
                    .trim_end_matches(';')
                    .to_owned(),
            ),
        })));
    } else {
        return Err(syntax(&c.near()));
    };
    let _ = c.punct(";");
    if c.i < c.toks.len() {
        return Err(syntax(&c.near()));
    }
    Ok(Some(Stmt::Alter(AlterInherited {
        kind,
        db,
        name,
        target,
        op,
    })))
}

/// An AMQP topology statement (chapter 50, "SQL, logic, MongoDB and
/// HTTP"), as `FOR EACH TENANT` definitions use it (the clause cut out
/// first).
#[derive(Debug, Clone, PartialEq)]
pub enum MqDecl {
    /// `CREATE EXCHANGE [IF NOT EXISTS] [db.]e [TYPE] t [INTERNAL] [WITH (...)]`.
    Exchange {
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// `db.`
        db: Option<String>,
        /// The name.
        name: String,
        /// The type.
        kind: String,
        /// `INTERNAL`.
        internal: bool,
        /// `WITH (k = v, ...)`, keys with `-` for `_`.
        arguments: Vec<(String, serde_json::Value)>,
    },
    /// `CREATE QUEUE [IF NOT EXISTS] [db.]q [WITH (...)]`.
    Queue {
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
        /// `db.`
        db: Option<String>,
        /// The name.
        name: String,
        /// `WITH (x_queue_type = 'quorum', ...)`.
        arguments: Vec<(String, serde_json::Value)>,
    },
    /// `BIND QUEUE | EXCHANGE [db.]d TO EXCHANGE s [KEY 'k'] [WITH (...)]`,
    /// or `UNBIND ... FROM EXCHANGE s [KEY 'k']` (`bind` false).
    Bind {
        /// Bind or unbind.
        bind: bool,
        /// `db.`
        db: Option<String>,
        /// `queue` or `exchange`.
        dest_kind: String,
        /// The destination.
        dest: String,
        /// The source exchange.
        source: String,
        /// The key.
        key: String,
        /// Arguments.
        arguments: Vec<(String, serde_json::Value)>,
    },
}

/// `WITH (k = v, ...)`: keys with `-` for `_` (`x_queue_type` is
/// `x-queue-type`), values typed (numbers, `TRUE`/`FALSE`, strings).
fn mq_args(c: &mut Cur<'_>) -> Result<Vec<(String, serde_json::Value)>> {
    if !c.punct("(") {
        return Err(syntax(&c.near()));
    }
    let mut out = Vec::new();
    if c.punct(")") {
        return Ok(out);
    }
    loop {
        let k = c.name()?.replace('_', "-");
        if !c.punct("=") {
            return Err(syntax(&c.near()));
        }
        let neg = c.punct("-");
        let t = *c.peek_at(0).ok_or_else(|| syntax(""))?;
        let text = c.text(&t);
        let v = match t.kind {
            TokenKind::String => serde_json::Value::String(
                string_value(t.bytes(c.src.as_bytes()), false)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .ok_or_else(|| syntax(text))?,
            ),
            TokenKind::Word if text.eq_ignore_ascii_case("true") => serde_json::Value::Bool(true),
            TokenKind::Word if text.eq_ignore_ascii_case("false") => serde_json::Value::Bool(false),
            _ => match text.parse::<i64>() {
                Ok(n) => serde_json::Value::from(if neg { -n } else { n }),
                Err(_) => match text.parse::<f64>() {
                    Ok(f) => serde_json::json!(if neg { -f } else { f }),
                    Err(_) => serde_json::Value::String(text.to_owned()),
                },
            },
        };
        c.i += 1;
        out.push((k, v));
        if c.punct(",") {
            continue;
        }
        if c.punct(")") {
            break;
        }
        return Err(syntax(&c.near()));
    }
    Ok(out)
}

/// An exchange, queue, bind or unbind statement; `None` for anything else.
///
/// # Errors
/// 1064 for a malformed one.
pub fn mq_decl(sql: &str, opts: &LexOptions) -> Result<Option<MqDecl>> {
    let mut c = Cur::new(sql, opts)?;
    let end = |c: &mut Cur<'_>| -> Result<()> {
        let _ = c.punct(";");
        if c.i < c.toks.len() {
            return Err(syntax(&c.near()));
        }
        Ok(())
    };
    if c.eat("create") {
        let exchange = if c.eat("exchange") {
            true
        } else if c.eat("queue") {
            false
        } else {
            return Ok(None);
        };
        let mut if_not_exists = false;
        if c.word_at(0, "if") && c.word_at(1, "not") && c.word_at(2, "exists") {
            c.i += 3;
            if_not_exists = true;
        }
        let (db, name) = c.qualified(false)?;
        if exchange {
            let _ = c.eat("type");
            let kind = c.name()?.to_ascii_lowercase();
            let internal = c.eat("internal");
            let arguments = if c.eat("with") {
                mq_args(&mut c)?
            } else {
                Vec::new()
            };
            end(&mut c)?;
            return Ok(Some(MqDecl::Exchange {
                if_not_exists,
                db,
                name,
                kind,
                internal,
                arguments,
            }));
        }
        let arguments = if c.eat("with") {
            mq_args(&mut c)?
        } else {
            Vec::new()
        };
        end(&mut c)?;
        return Ok(Some(MqDecl::Queue {
            if_not_exists,
            db,
            name,
            arguments,
        }));
    }
    let bind = if c.eat("bind") {
        true
    } else if c.eat("unbind") {
        false
    } else {
        return Ok(None);
    };
    let dest_kind = if c.eat("queue") {
        "queue"
    } else if c.eat("exchange") {
        "exchange"
    } else {
        return Err(syntax(&c.near()));
    };
    let (db, dest) = c.qualified(false)?;
    if !(c.eat(if bind { "to" } else { "from" }) && c.eat("exchange")) {
        return Err(syntax(&c.near()));
    }
    let (_, source) = c.qualified(false)?;
    let key = if c.eat("key") {
        c.name()?
    } else {
        String::new()
    };
    let arguments = if c.eat("with") {
        mq_args(&mut c)?
    } else {
        Vec::new()
    };
    end(&mut c)?;
    Ok(Some(MqDecl::Bind {
        bind,
        db,
        dest_kind: dest_kind.to_owned(),
        dest,
        source,
        key,
        arguments,
    }))
}

/// The kind a `CREATE [OR REPLACE] <kind>` or `DROP <kind>` names, with
/// whether it is a drop.
#[must_use]
pub fn ddl_kind(sql: &str, opts: &LexOptions) -> Option<(super::model::DefKind, bool)> {
    let mut c = Cur::new(sql, opts).ok()?;
    let drop = if c.eat("create") {
        if c.word_at(0, "or") && c.word_at(1, "replace") {
            c.i += 2;
        }
        false
    } else if c.eat("drop") {
        true
    } else if c.eat("alter") {
        false
    } else {
        return None;
    };
    let t = *c.peek_at(0)?;
    let k = super::model::DefKind::parse(c.text(&t))?;
    Some((k, drop))
}

/// A quick test on the raw text: could the statement be this module's?
#[must_use]
pub fn maybe(sql: &[u8]) -> bool {
    let has = |w: &[u8]| sql.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
    has(b"TENANT") || has(b"INHERITED")
}
