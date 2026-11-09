//! The declaration surface (chapter 41, "Declaring code"; build step 3,
//! functions and procedures):
//!
//! ```text
//! CREATE [OR REPLACE] {FUNCTION | PROCEDURE} [db.]name [(params)] [RETURNS type]
//!   LANGUAGE {kirun | javascript}
//!   [SQL SECURITY {INVOKER | DEFINER}] [[NOT] DETERMINISTIC]
//!   [EXPOSE (sql, http, ...)] [WITH (SPRIG_TIER = 'auto' | 'mariadb' | 'engine')]
//!   [COMMENT 'text']
//! AS {'text' | "text" | $$ text $$}
//! ```
//!
//! Clauses may come in any order before `AS`. A KIRun procedure's
//! signature comes from its definition, so its parameter list is ignored.
//! `LANGUAGE sql` (or no `LANGUAGE`) is ordinary DDL and not parsed here.

use serde::Serialize;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_compile::Runtime;

/// What a declaration declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// `CREATE FUNCTION`.
    Function,
    /// `CREATE PROCEDURE`.
    Procedure,
    /// `CREATE VALIDATION v ON t FOR INSERT, UPDATE` (R7 order 3).
    Validation,
    /// `CREATE TRIGGER g AFTER COMMIT ON t FOR ...` (R7 order 5).
    Trigger,
    /// `CREATE SCHEDULE s CRON '...' DO CALL p(...)` (R7 order 5).
    Schedule,
}

impl Kind {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Kind::Function => "function",
            Kind::Procedure => "procedure",
            Kind::Validation => "validation",
            Kind::Trigger => "trigger",
            Kind::Schedule => "schedule",
        }
    }

    /// Whether code of this kind always runs with its definer's rights
    /// (chapter 41, "Privileges and transactions").
    #[must_use]
    pub const fn definer_only(self) -> bool {
        matches!(self, Kind::Validation | Kind::Trigger | Kind::Schedule)
    }
}

/// A table a validation or trigger is bound to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableRef {
    /// The database, if the name was qualified.
    pub db: Option<String>,
    /// The table.
    pub name: String,
}

/// `CREATE SCHEDULE`'s clauses.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ScheduleSpec {
    /// The cron text (five fields).
    pub cron: String,
    /// `TIME ZONE`, IANA name; `UTC` by default.
    pub time_zone: String,
    /// `OVERLAP SKIP` (default) or `OVERLAP QUEUE`.
    pub queue_one: bool,
    /// The procedure it calls.
    pub target: TableRef,
    /// The call's arguments, as JSON values.
    pub args: Vec<serde_json::Value>,
}

/// `SPRIG_TIER`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TierPref {
    /// Placement decides.
    Auto,
    /// MariaDB or refuse (39868).
    Mariadb,
    /// Always the engine tier.
    Engine,
}

impl TierPref {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            TierPref::Auto => "auto",
            TierPref::Mariadb => "mariadb",
            TierPref::Engine => "engine",
        }
    }
}

/// A parsed declaration.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Declaration {
    /// Function or procedure.
    pub kind: Kind,
    /// `OR REPLACE` was given.
    pub or_replace: bool,
    /// The database, if the name was qualified.
    pub db: Option<String>,
    /// The name.
    pub name: String,
    /// The language.
    pub runtime: Runtime,
    /// `SQL SECURITY DEFINER`.
    pub definer_security: bool,
    /// `DETERMINISTIC`.
    pub deterministic: bool,
    /// `EXPOSE (...)`, lower case; `["sql"]` by default.
    pub exposure: Vec<String>,
    /// `SPRIG_TIER`.
    pub tier: TierPref,
    /// The body.
    pub body: String,
    /// Validations and triggers: the table.
    pub table: Option<TableRef>,
    /// Validations and triggers: `insert`, `update`, `delete`.
    pub events: Vec<String>,
    /// Triggers: `FOR UPDATE OF (c, ...)`, fire only when these changed.
    pub when_fields: Vec<String>,
    /// Schedules.
    pub schedule: Option<ScheduleSpec>,
}

fn syntax(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicSyntax, m)
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(String),
    /// A quoted name.
    Name(String),
    Str(String),
    Punct(char),
    Body(String),
}

fn lex(src: &str) -> Result<Vec<Tok>> {
    let mut out = Vec::new();
    let cs: Vec<char> = src.chars().collect();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '-' && cs.get(i + 1) == Some(&'-') {
            while i < cs.len() && cs[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && cs.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < cs.len() && !(cs[i] == '*' && cs[i + 1] == '/') {
                i += 1;
            }
            i += 2;
            continue;
        }
        if c == '$' && cs.get(i + 1) == Some(&'$') {
            let start = i + 2;
            let mut j = start;
            while j + 1 < cs.len() && !(cs[j] == '$' && cs[j + 1] == '$') {
                j += 1;
            }
            if j + 1 >= cs.len() {
                return Err(syntax("unterminated $$ body"));
            }
            out.push(Tok::Body(cs[start..j].iter().collect()));
            i = j + 2;
            continue;
        }
        if c == '\'' || c == '"' || c == '`' {
            let q = c;
            let mut s = String::new();
            i += 1;
            loop {
                let Some(&ch) = cs.get(i) else {
                    return Err(syntax("unterminated quoted text"));
                };
                i += 1;
                if ch == q {
                    if cs.get(i) == Some(&q) {
                        s.push(q);
                        i += 1;
                        continue;
                    }
                    break;
                }
                if ch == '\\' && q != '`' {
                    let Some(&e) = cs.get(i) else {
                        return Err(syntax("unterminated quoted text"));
                    };
                    i += 1;
                    s.push(match e {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        '0' => '\0',
                        other => other,
                    });
                    continue;
                }
                s.push(ch);
            }
            out.push(if q == '`' { Tok::Name(s) } else { Tok::Str(s) });
            continue;
        }
        if c.is_alphanumeric() || c == '_' {
            let start = i;
            while i < cs.len() && (cs[i].is_alphanumeric() || cs[i] == '_' || cs[i] == '$') {
                i += 1;
            }
            out.push(Tok::Word(cs[start..i].iter().collect()));
            continue;
        }
        out.push(Tok::Punct(c));
        i += 1;
    }
    Ok(out)
}

fn is_kw(t: Option<&Tok>, kw: &str) -> bool {
    matches!(t, Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw))
}

/// Whether `sql` is a logic declaration, cheaply, without full parsing:
/// `CREATE [OR REPLACE] FUNCTION | PROCEDURE ... LANGUAGE kirun |
/// javascript`, `CREATE [OR REPLACE] VALIDATION`, `CREATE [OR REPLACE]
/// TRIGGER ... AFTER COMMIT` or `CREATE [OR REPLACE] SCHEDULE`.
#[must_use]
pub fn is_declaration(sql: &str) -> bool {
    let head: String = sql
        .chars()
        .take(4096)
        .collect::<String>()
        .to_ascii_lowercase();
    let t = head.trim_start();
    if !t.starts_with("create") {
        return false;
    }
    let words: Vec<&str> = t
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .filter(|w| !w.is_empty())
        .collect();
    let kind = match words.get(1) {
        Some(&"or") if words.get(2) == Some(&"replace") => words.get(3),
        w => w,
    };
    match kind {
        Some(&"validation" | &"schedule") => true,
        Some(&"trigger") => words.windows(2).any(|w| w == ["after", "commit"]),
        Some(&"function" | &"procedure") => words
            .windows(2)
            .any(|w| w[0] == "language" && matches!(w[1], "kirun" | "javascript" | "js")),
        _ => false,
    }
}

struct P {
    toks: Vec<Tok>,
    i: usize,
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.i).cloned();
        self.i += 1;
        t
    }

    fn kw(&self, kw: &str) -> bool {
        is_kw(self.peek(), kw)
    }

    fn eat(&mut self, kw: &str) -> bool {
        if self.kw(kw) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, kw: &str) -> Result<()> {
        if self.eat(kw) {
            Ok(())
        } else {
            Err(syntax(format!("expected {}", kw.to_ascii_uppercase())))
        }
    }

    fn punct(&mut self, c: char) -> bool {
        if self.peek() == Some(&Tok::Punct(c)) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn ident(&mut self) -> Result<String> {
        match self.next() {
            Some(Tok::Word(w) | Tok::Name(w)) => Ok(w),
            _ => Err(syntax("expected a name")),
        }
    }

    /// `[db.]name`.
    fn qualified(&mut self) -> Result<TableRef> {
        let first = self.ident()?;
        if self.punct('.') {
            Ok(TableRef {
                db: Some(first),
                name: self.ident()?,
            })
        } else {
            Ok(TableRef {
                db: None,
                name: first,
            })
        }
    }

    /// `(a, b, ...)` of names.
    fn name_list(&mut self) -> Result<Vec<String>> {
        if !self.punct('(') {
            return Err(syntax("expected ("));
        }
        let mut out = Vec::new();
        loop {
            out.push(self.ident()?);
            if self.punct(')') {
                return Ok(out);
            }
            if !self.punct(',') {
                return Err(syntax("expected , or )"));
            }
        }
    }

    /// `FOR INSERT, UPDATE [OF (c, ...)], DELETE`.
    fn events(&mut self, allow_delete: bool, allow_of: bool) -> Result<(Vec<String>, Vec<String>)> {
        self.expect("for")?;
        let mut events = Vec::new();
        let mut when = Vec::new();
        loop {
            let ev = match self.next() {
                Some(Tok::Word(w)) => w.to_ascii_lowercase(),
                _ => return Err(syntax("expected INSERT, UPDATE or DELETE")),
            };
            match ev.as_str() {
                "insert" | "update" => {}
                "delete" if allow_delete => {}
                _ => {
                    return Err(syntax(format!(
                        "FOR takes INSERT, UPDATE{}",
                        if allow_delete { " or DELETE" } else { "" }
                    )));
                }
            }
            if ev == "update" && allow_of && self.eat("of") {
                when = self.name_list()?;
            }
            if !events.contains(&ev) {
                events.push(ev);
            }
            if !self.punct(',') {
                return Ok((events, when));
            }
        }
    }

    /// One literal argument of `DO CALL p(...)`.
    fn literal(&mut self) -> Result<serde_json::Value> {
        let neg = self.punct('-');
        match self.next() {
            Some(Tok::Str(s)) if !neg => Ok(serde_json::Value::String(s)),
            Some(Tok::Word(w)) => {
                let l = w.to_ascii_lowercase();
                match l.as_str() {
                    "null" if !neg => Ok(serde_json::Value::Null),
                    "true" if !neg => Ok(serde_json::Value::Bool(true)),
                    "false" if !neg => Ok(serde_json::Value::Bool(false)),
                    _ => {
                        // A number; `1.5` lexes as `1`, `.`, `5`.
                        let mut text = if neg { format!("-{w}") } else { w };
                        if self.peek() == Some(&Tok::Punct('.')) {
                            self.i += 1;
                            if let Some(Tok::Word(f)) = self.next() {
                                text = format!("{text}.{f}");
                            }
                        }
                        if let Ok(i) = text.parse::<i64>() {
                            Ok(serde_json::Value::from(i))
                        } else if let Ok(f) = text.parse::<f64>() {
                            Ok(serde_json::Value::from(f))
                        } else {
                            Err(syntax(format!("CALL takes constant arguments, not {text}")))
                        }
                    }
                }
            }
            _ => Err(syntax("CALL takes constant arguments")),
        }
    }

    fn string(&mut self, what: &str) -> Result<String> {
        match self.next() {
            Some(Tok::Str(s)) => Ok(s),
            _ => Err(syntax(format!("{what} takes a string"))),
        }
    }
}

/// Parses a declaration.
///
/// # Errors
/// 39861 for text that is not a declaration of this form.
pub fn parse(sql: &str) -> Result<Declaration> {
    let mut p = P {
        toks: lex(sql.trim().trim_end_matches(';'))?,
        i: 0,
    };
    if !p.eat("create") {
        return Err(syntax("a declaration starts with CREATE"));
    }
    let or_replace = if p.kw("or") {
        p.i += 1;
        p.expect("replace")?;
        true
    } else {
        false
    };
    let kind = match p.next() {
        Some(Tok::Word(w)) => match w.to_ascii_lowercase().as_str() {
            "function" => Kind::Function,
            "procedure" => Kind::Procedure,
            "validation" => Kind::Validation,
            "trigger" => Kind::Trigger,
            "schedule" => Kind::Schedule,
            _ => {
                return Err(syntax(
                    "expected FUNCTION, PROCEDURE, VALIDATION, TRIGGER or SCHEDULE \
                     (DEFINER= isn't allowed here)",
                ));
            }
        },
        _ => return Err(syntax("expected FUNCTION or PROCEDURE")),
    };
    let named = p.qualified()?;
    if named.name.is_empty() || named.name.chars().count() > 64 {
        return Err(syntax("a routine name has 1 to 64 characters"));
    }
    let mut d = Declaration {
        kind,
        or_replace,
        db: named.db,
        name: named.name,
        runtime: Runtime::Sql,
        definer_security: kind.definer_only(),
        deterministic: false,
        exposure: vec!["sql".to_owned()],
        tier: TierPref::Auto,
        body: String::new(),
        table: None,
        events: Vec::new(),
        when_fields: Vec::new(),
        schedule: None,
    };
    match kind {
        Kind::Function | Kind::Procedure => {
            // A parameter list, skipped (balanced parentheses).
            if p.peek() == Some(&Tok::Punct('(')) {
                let mut depth = 0;
                loop {
                    match p.next() {
                        Some(Tok::Punct('(')) => depth += 1,
                        Some(Tok::Punct(')')) => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        Some(_) => {}
                        None => return Err(syntax("unbalanced parameter list")),
                    }
                }
            }
        }
        Kind::Validation => {
            p.expect("on")?;
            d.table = Some(p.qualified()?);
            let (ev, _) = p.events(false, false)?;
            d.events = ev;
        }
        Kind::Trigger => {
            p.expect("after")?;
            p.expect("commit")?;
            p.expect("on")?;
            d.table = Some(p.qualified()?);
            let (ev, when) = p.events(true, true)?;
            d.events = ev;
            d.when_fields = when;
        }
        Kind::Schedule => {
            p.expect("cron")?;
            let cron = p.string("CRON")?;
            let mut time_zone = "UTC".to_owned();
            let mut queue_one = false;
            loop {
                if p.eat("time") {
                    p.expect("zone")?;
                    time_zone = p.string("TIME ZONE")?;
                } else if p.eat("overlap") {
                    queue_one = match p.next() {
                        Some(Tok::Word(w)) if w.eq_ignore_ascii_case("skip") => false,
                        Some(Tok::Word(w)) if w.eq_ignore_ascii_case("queue") => true,
                        _ => return Err(syntax("OVERLAP takes SKIP or QUEUE")),
                    };
                } else {
                    break;
                }
            }
            p.expect("do")?;
            p.expect("call")?;
            let target = p.qualified()?;
            let mut args = Vec::new();
            if p.punct('(') && !p.punct(')') {
                loop {
                    args.push(p.literal()?);
                    if p.punct(')') {
                        break;
                    }
                    if !p.punct(',') {
                        return Err(syntax("expected , or ) in CALL"));
                    }
                }
            }
            if p.peek().is_some() {
                return Err(syntax("text after the CALL"));
            }
            d.runtime = Runtime::Sql;
            d.schedule = Some(ScheduleSpec {
                cron,
                time_zone,
                queue_one,
                target,
                args,
            });
            return Ok(d);
        }
    }
    let mut language = None;
    loop {
        let Some(t) = p.next() else {
            return Err(syntax("expected AS and a body"));
        };
        let Tok::Word(w) = t else {
            return Err(syntax(format!("unexpected {t:?}")));
        };
        match w.to_ascii_lowercase().as_str() {
            "returns" => {
                // A type: a word and an optional (n[, m]).
                p.next();
                if p.peek() == Some(&Tok::Punct('(')) {
                    while !matches!(p.next(), Some(Tok::Punct(')')) | None) {}
                }
            }
            "language" => {
                let l = p.ident()?;
                language = Some(
                    sprig_logic_compile::Runtime::parse(&l)
                        .ok_or_else(|| syntax(format!("unknown LANGUAGE {l}")))?,
                );
            }
            "sql" => {
                p.expect("security")?;
                let v = p.ident()?.to_ascii_lowercase();
                d.definer_security = match v.as_str() {
                    "definer" => true,
                    "invoker" if kind.definer_only() => {
                        return Err(syntax(format!(
                            "a {} always runs with its definer's rights",
                            kind.as_str()
                        )));
                    }
                    "invoker" => false,
                    _ => return Err(syntax("SQL SECURITY takes INVOKER or DEFINER")),
                };
            }
            "deterministic" => d.deterministic = true,
            "not" => p.expect("deterministic")?,
            "comment" => {
                p.string("COMMENT")?;
            }
            "expose" => {
                if !p.punct('(') {
                    return Err(syntax("EXPOSE takes a list"));
                }
                let mut list = Vec::new();
                loop {
                    match p.next() {
                        Some(Tok::Word(x)) => {
                            let x = x.to_ascii_lowercase();
                            if !matches!(x.as_str(), "sql" | "http" | "grpc" | "mongo" | "redis") {
                                return Err(syntax(format!("EXPOSE: unknown protocol {x}")));
                            }
                            list.push(x);
                        }
                        Some(Tok::Punct(',')) => {}
                        Some(Tok::Punct(')')) => break,
                        _ => return Err(syntax("EXPOSE: bad list")),
                    }
                }
                d.exposure = list;
            }
            "with" => {
                if !p.punct('(') || !p.eat("sprig_tier") {
                    return Err(syntax("expected WITH (SPRIG_TIER = ...)"));
                }
                if !p.punct('=') {
                    return Err(syntax("expected SPRIG_TIER = '...'"));
                }
                d.tier = match p.next() {
                    Some(Tok::Str(v) | Tok::Word(v)) => match v.to_ascii_lowercase().as_str() {
                        "auto" => TierPref::Auto,
                        "mariadb" => TierPref::Mariadb,
                        "engine" => TierPref::Engine,
                        _ => return Err(syntax("SPRIG_TIER is 'auto', 'mariadb' or 'engine'")),
                    },
                    _ => return Err(syntax("SPRIG_TIER takes a string")),
                };
                if !p.punct(')') {
                    return Err(syntax("expected )"));
                }
            }
            "as" => {
                d.body = match p.next() {
                    Some(Tok::Str(b) | Tok::Body(b)) => b,
                    _ => return Err(syntax("AS takes a quoted or $$ body")),
                };
                if p.peek().is_some() {
                    return Err(syntax("text after the body"));
                }
                break;
            }
            other => return Err(syntax(format!("unexpected {other}"))),
        }
    }
    d.runtime = language.ok_or_else(|| syntax("LANGUAGE is missing"))?;
    Ok(d)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn kirun_procedure() {
        let s = "CREATE OR REPLACE PROCEDURE shop.quote LANGUAGE kirun SQL SECURITY DEFINER \
                 WITH (SPRIG_TIER = 'mariadb') EXPOSE (sql, http) AS '{\"name\": \"Q\", \"x\": \"it''s\"}';";
        assert!(is_declaration(s));
        let d = parse(s).unwrap();
        assert_eq!(d.kind, Kind::Procedure);
        assert!(d.or_replace);
        assert_eq!(d.db.as_deref(), Some("shop"));
        assert_eq!(d.name, "quote");
        assert_eq!(d.runtime, Runtime::Kirun);
        assert!(d.definer_security);
        assert_eq!(d.tier, TierPref::Mariadb);
        assert_eq!(d.exposure, vec!["sql", "http"]);
        assert_eq!(d.body, "{\"name\": \"Q\", \"x\": \"it's\"}");
    }

    #[test]
    fn javascript_function_with_dollar_body() {
        let s = "create function `order total`(id BIGINT) returns decimal(12, 2) language javascript \
                 deterministic as $$ export default (id) => id * 2; $$";
        let d = parse(s).unwrap();
        assert_eq!(d.kind, Kind::Function);
        assert_eq!(d.name, "order total");
        assert_eq!(d.runtime, Runtime::Javascript);
        assert!(d.deterministic);
        assert_eq!(d.body.trim(), "export default (id) => id * 2;");
    }

    #[test]
    fn ordinary_ddl_is_not_a_declaration() {
        assert!(!is_declaration("CREATE PROCEDURE p() BEGIN SELECT 1; END"));
        assert!(!is_declaration(
            "CREATE FUNCTION f() RETURNS INT LANGUAGE SQL RETURN 1"
        ));
        assert!(!is_declaration("SELECT 'language kirun'"));
    }

    #[test]
    fn validations_triggers_and_schedules() {
        let v = "CREATE OR REPLACE VALIDATION shop.discount_rules ON shop.orders FOR INSERT, UPDATE \
                 LANGUAGE kirun AS '{}'";
        assert!(is_declaration(v));
        let d = parse(v).unwrap();
        assert_eq!(d.kind, Kind::Validation);
        assert!(d.definer_security);
        assert_eq!(d.table.as_ref().unwrap().name, "orders");
        assert_eq!(d.events, ["insert", "update"]);

        let t = "create trigger notify after commit on orders for insert, update of (status, total), delete \
                 language kirun as '{}'";
        assert!(is_declaration(t));
        let d = parse(t).unwrap();
        assert_eq!(d.kind, Kind::Trigger);
        assert_eq!(d.events, ["insert", "update", "delete"]);
        assert_eq!(d.when_fields, ["status", "total"]);
        assert!(!is_declaration(
            "CREATE TRIGGER t AFTER INSERT ON x FOR EACH ROW SET @a = 1"
        ));

        let s = "CREATE SCHEDULE shop.nightly CRON '0 2 * * *' TIME ZONE 'Asia/Kolkata' OVERLAP QUEUE \
                 DO CALL shop.report(7, -1.5, 'x', NULL)";
        assert!(is_declaration(s));
        let d = parse(s).unwrap();
        assert_eq!(d.kind, Kind::Schedule);
        let sc = d.schedule.unwrap();
        assert_eq!(sc.cron, "0 2 * * *");
        assert_eq!(sc.time_zone, "Asia/Kolkata");
        assert!(sc.queue_one);
        assert_eq!(sc.target.name, "report");
        assert_eq!(
            serde_json::Value::from(sc.args),
            serde_json::json!([7, -1.5, "x", null])
        );
        assert_eq!(
            parse("CREATE VALIDATION v ON t FOR INSERT LANGUAGE kirun SQL SECURITY INVOKER AS '1'")
                .unwrap_err()
                .code(),
            Some(SprigCode::LogicSyntax)
        );
    }

    #[test]
    fn errors() {
        for s in [
            "CREATE PROCEDURE p LANGUAGE kirun",
            "CREATE PROCEDURE p LANGUAGE cobol AS '1'",
            "CREATE PROCEDURE p AS '1'",
            "CREATE PROCEDURE p LANGUAGE kirun WITH (SPRIG_TIER = 'gpu') AS '1'",
            "CREATE PROCEDURE p LANGUAGE kirun AS '1' extra",
            "CREATE DEFINER=x PROCEDURE p LANGUAGE kirun AS '1'",
        ] {
            assert_eq!(
                parse(s).unwrap_err().code(),
                Some(SprigCode::LogicSyntax),
                "{s}"
            );
        }
    }
}
