//! What a statement does to session state and routing: [`StmtInfo`].
//!
//! Chapter 06 takes these facts from parsing (chapter 17's parser, R1).
//! Until then [`StmtInfo::classify`] derives them from a conservative
//! token scan: a statement counts as a plain read only when it starts with
//! `SELECT`, `WITH`, `VALUES`, `TABLE`, `DESCRIBE` or `EXPLAIN` and has no
//! locking clause, no `INTO`, no user-variable assignment and no call of a
//! function with side effects; anything not understood is a write and goes
//! to the writer. The pass-through executor (decision D3) can use it as is;
//! a real parser fills the same struct.

/// Where a statement may run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtClass {
    /// A plain read: may leave the writer when the session has no pins.
    Read,
    /// Anything else: runs on the writer (or the pinned connection).
    Write,
}

/// A session-state effect of a statement (chapter 06, "Replay rules").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// `BEGIN`, `START TRANSACTION`.
    Begin,
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK` (not `ROLLBACK TO SAVEPOINT`).
    Rollback,
    /// DDL and other statements that commit implicitly.
    ImplicitCommit,
    /// `SET TRANSACTION ...` for the next transaction only.
    NextTransaction,
    /// `CREATE TEMPORARY TABLE name`.
    CreateTemp(String),
    /// `DROP [TEMPORARY] TABLE names`: drops any of them that are
    /// temporary.
    DropTables(Vec<String>),
    /// `LOCK TABLES`.
    LockTables,
    /// `UNLOCK TABLES`.
    UnlockTables,
    /// A `GET_LOCK()` call.
    GetLock,
    /// A `RELEASE_LOCK()` call.
    ReleaseLock,
    /// `RELEASE_ALL_LOCKS()`.
    ReleaseAllLocks,
    /// `HANDLER ... OPEN`.
    HandlerOpen,
    /// `HANDLER ... CLOSE`.
    HandlerClose,
    /// Client `XA START`.
    XaStart,
    /// `XA COMMIT` or `XA ROLLBACK`.
    XaEnd,
    /// State SprigDB can't replay: a session variable outside the allowed
    /// list, SQL-level `PREPARE`, `LASTVAL()`, `PREVIOUS VALUE FOR`.
    Untracked(String),
    /// `CALL`: a routine may change anything; session tracking tells.
    Call,
    /// `SET NAMES`: the client character set takes the collation it gives
    /// `collation_connection` (chapter 06, "Login collation").
    SetNames,
}

/// The facts about one statement that pools, sync and routing need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StmtInfo {
    /// Plain read or not.
    pub class: StmtClass,
    /// User variables the statement reads (lower case, without `@`).
    pub reads_user_vars: Vec<String>,
    /// It may read any user variable (`CALL`).
    pub reads_all_user_vars: bool,
    /// User variables it assigns from expressions, captured before the
    /// connection is released.
    pub assigns_user_vars: Vec<String>,
    /// It calls `LAST_INSERT_ID()` without an argument.
    pub uses_last_insert_id: bool,
    /// It uses `SQL_CALC_FOUND_ROWS`.
    pub calc_found_rows: bool,
    /// Session-state effects.
    pub effects: Vec<Effect>,
    /// Variables set only for this statement (`SET STATEMENT a = 1, b = 2
    /// FOR ...`, lower case). MariaDB's session tracking reports their
    /// temporary values as changes, though they revert when the statement
    /// ends: those reports are ignored.
    pub scoped_vars: Vec<String>,
    /// Gateway-owned variables it sets (session tracking, timeouts,
    /// `wsrep_sync_wait`; lower case). The gateway keeps the client's
    /// values and restores its own on the connection afterwards.
    pub sets_gateway_vars: Vec<String>,
}

impl StmtInfo {
    /// A write with no known effects.
    #[must_use]
    pub fn write() -> Self {
        Self {
            class: StmtClass::Write,
            reads_user_vars: Vec::new(),
            reads_all_user_vars: false,
            assigns_user_vars: Vec::new(),
            uses_last_insert_id: false,
            calc_found_rows: false,
            effects: Vec::new(),
            scoped_vars: Vec::new(),
            sets_gateway_vars: Vec::new(),
        }
    }

    /// A plain read with no known effects.
    #[must_use]
    pub fn read() -> Self {
        Self {
            class: StmtClass::Read,
            ..Self::write()
        }
    }

    /// Whether committing is part of the statement (`COMMIT`, or an
    /// implicit commit): losing the node during it leaves the outcome
    /// unknown.
    #[must_use]
    pub fn commits(&self) -> bool {
        self.effects
            .iter()
            .any(|e| matches!(e, Effect::Commit | Effect::ImplicitCommit | Effect::XaEnd))
    }

    /// Whether it is `SET NAMES`.
    #[must_use]
    pub fn sets_names(&self) -> bool {
        self.effects.contains(&Effect::SetNames)
    }

    /// Whether the statement predicts a change that session tracking may
    /// flag (`session_track_state_change`).
    #[must_use]
    pub fn expects_state_change(&self) -> bool {
        !self.assigns_user_vars.is_empty()
            || !self.scoped_vars.is_empty()
            || !self.sets_gateway_vars.is_empty()
            || self.effects.iter().any(|e| !matches!(e, Effect::Call))
    }

    /// Classifies statement text conservatively (see the module docs).
    #[must_use]
    pub fn classify(sql: &[u8]) -> Self {
        Self::classify_tokens(&tokenize(sql))
    }

    /// `SET STATEMENT a = 1, ... FOR <statement>`: the inner statement's
    /// facts, plus the variables set for it alone and the user variables
    /// the assignments read.
    fn classify_set_statement(toks: &[Tok]) -> Self {
        let mut depth = 0i32;
        let mut for_at = None;
        for (i, t) in toks.iter().enumerate().skip(2) {
            match t {
                Tok::Punct(p) if p == "(" => depth += 1,
                Tok::Punct(p) if p == ")" => depth -= 1,
                Tok::Word(w) if w == "FOR" && depth == 0 => {
                    for_at = Some(i);
                    break;
                }
                _ => {}
            }
        }
        let Some(f) = for_at else {
            return StmtInfo::write();
        };
        let mut info = Self::classify_tokens(&toks[f + 1..]);
        let mut at_start = true;
        depth = 0;
        for t in &toks[2..f] {
            match t {
                Tok::Punct(p) if p == "(" => depth += 1,
                Tok::Punct(p) if p == ")" => depth -= 1,
                Tok::Punct(p) if p == "," && depth == 0 => {
                    at_start = true;
                    continue;
                }
                Tok::UserVar(v) => push_unique(&mut info.reads_user_vars, v),
                _ => {}
            }
            if at_start && depth == 0 {
                match t {
                    Tok::Word(w) if w == "SESSION" || w == "LOCAL" => continue,
                    Tok::SysVar(_, name) => push_unique(&mut info.scoped_vars, name),
                    t => {
                        if let Some(name) = t.ident() {
                            push_unique(&mut info.scoped_vars, &name);
                        }
                    }
                }
                at_start = false;
            }
        }
        info
    }

    #[allow(clippy::too_many_lines)] // one table of statement kinds
    fn classify_tokens(toks: &[Tok]) -> Self {
        let mut info = StmtInfo::write();
        let kw = |i: usize| toks.get(i).and_then(Tok::word);
        let Some(first) = kw(0) else {
            return info;
        };
        if first == "SET" && kw(1) == Some("STATEMENT") {
            return Self::classify_set_statement(toks);
        }

        // User variables: reads, and assignments by `@v :=` or `INTO @v`.
        let mut in_into = false;
        for (i, t) in toks.iter().enumerate() {
            match t {
                Tok::Word(w) if w == "INTO" => in_into = true,
                Tok::Punct(p) if p == "," => {}
                Tok::UserVar(v) => {
                    let assigned = matches!(toks.get(i + 1), Some(Tok::Punct(p)) if p == ":=");
                    if assigned || in_into {
                        push_unique(&mut info.assigns_user_vars, v);
                    } else {
                        push_unique(&mut info.reads_user_vars, v);
                    }
                }
                _ => in_into = false,
            }
        }
        // Functions with session effects, anywhere.
        let mut side_effects = false;
        for (i, t) in toks.iter().enumerate() {
            let Some(w) = t.word() else { continue };
            let call = matches!(toks.get(i + 1), Some(Tok::Punct(p)) if p == "(");
            match w {
                "GET_LOCK" if call => {
                    info.effects.push(Effect::GetLock);
                    side_effects = true;
                }
                "RELEASE_LOCK" if call => {
                    info.effects.push(Effect::ReleaseLock);
                    side_effects = true;
                }
                "RELEASE_ALL_LOCKS" if call => {
                    info.effects.push(Effect::ReleaseAllLocks);
                    side_effects = true;
                }
                "LASTVAL" if call => {
                    info.effects.push(Effect::Untracked("LASTVAL()".into()));
                    side_effects = true;
                }
                "PREVIOUS" if kw(i + 1) == Some("VALUE") && kw(i + 2) == Some("FOR") => {
                    info.effects
                        .push(Effect::Untracked("PREVIOUS VALUE FOR".into()));
                    side_effects = true;
                }
                "NEXTVAL" | "SETVAL" | "SLEEP" | "BENCHMARK" | "FOUND_ROWS" | "ROW_COUNT"
                    if call =>
                {
                    side_effects = true;
                }
                "NEXT" if kw(i + 1) == Some("VALUE") => side_effects = true,
                "LAST_INSERT_ID" if call => {
                    if matches!(toks.get(i + 2), Some(Tok::Punct(p)) if p == ")") {
                        info.uses_last_insert_id = true;
                    } else {
                        side_effects = true;
                    }
                }
                "SQL_CALC_FOUND_ROWS" => info.calc_found_rows = true,
                _ => {}
            }
        }
        let has = |w: &str| toks.iter().any(|t| t.word() == Some(w));
        let has_seq = |a: &str, b: &str| {
            toks.windows(2)
                .any(|p| p[0].word() == Some(a) && p[1].word() == Some(b))
        };

        match first {
            "SELECT" | "WITH" | "VALUES" | "TABLE" | "DESCRIBE" | "DESC" | "EXPLAIN" => {
                let locking = has_seq("FOR", "UPDATE")
                    || has_seq("FOR", "SHARE")
                    || has_seq("SHARE", "MODE")
                    || has("INTO");
                let explain_write =
                    matches!(first, "EXPLAIN" | "DESCRIBE" | "DESC") && has("ANALYZE");
                if !locking && !side_effects && info.assigns_user_vars.is_empty() && !explain_write
                {
                    info.class = StmtClass::Read;
                }
            }
            "SET" => classify_set(toks, &mut info),
            "BEGIN" if kw(1) != Some("NOT") => info.effects.push(Effect::Begin),
            "START" if kw(1) == Some("TRANSACTION") => info.effects.push(Effect::Begin),
            "COMMIT" => info.effects.push(Effect::Commit),
            "ROLLBACK" if !has("TO") => info.effects.push(Effect::Rollback),
            "XA" => match kw(1) {
                Some("START" | "BEGIN") => info.effects.push(Effect::XaStart),
                Some("COMMIT" | "ROLLBACK") => info.effects.push(Effect::XaEnd),
                _ => {}
            },
            "LOCK" if matches!(kw(1), Some("TABLES" | "TABLE")) => {
                info.effects.push(Effect::LockTables);
            }
            "UNLOCK" if matches!(kw(1), Some("TABLES" | "TABLE")) => {
                info.effects.push(Effect::UnlockTables);
            }
            "HANDLER" => {
                if has("OPEN") {
                    info.effects.push(Effect::HandlerOpen);
                } else if has("CLOSE") {
                    info.effects.push(Effect::HandlerClose);
                }
            }
            "CREATE"
                if kw(1) == Some("TEMPORARY")
                    || (kw(1) == Some("OR") && kw(3) == Some("TEMPORARY")) =>
            {
                if let Some(name) = name_after(toks, "TABLE") {
                    info.effects.push(Effect::CreateTemp(name));
                }
            }
            "DROP"
                if matches!(kw(1), Some("TABLE" | "TABLES"))
                    || (kw(1) == Some("TEMPORARY")
                        && matches!(kw(2), Some("TABLE" | "TABLES"))) =>
            {
                let temporary = kw(1) == Some("TEMPORARY");
                info.effects.push(Effect::DropTables(table_list(toks)));
                if !temporary {
                    info.effects.push(Effect::ImplicitCommit);
                }
            }
            "CREATE" | "ALTER" | "DROP" | "RENAME" | "TRUNCATE" | "GRANT" | "REVOKE"
            | "ANALYZE" | "OPTIMIZE" | "REPAIR" | "FLUSH" => {
                info.effects.push(Effect::ImplicitCommit);
            }
            "PREPARE" | "EXECUTE" | "DEALLOCATE" => {
                info.effects.push(Effect::Untracked(format!("SQL {first}")));
            }
            "CALL" => {
                info.reads_all_user_vars = true;
                info.effects.push(Effect::Call);
                // A user variable passed as an argument may be an OUT or
                // INOUT parameter: capture it after the call, so a read on
                // another connection (a replica, a Galera node) sees the
                // routine's value.
                for v in info.reads_user_vars.clone() {
                    push_unique(&mut info.assigns_user_vars, &v);
                }
            }
            _ => {}
        }
        info
    }
}

/// `SET` statements: user variables, session variables, names,
/// transaction characteristics, roles.
fn classify_set(toks: &[Tok], info: &mut StmtInfo) {
    let kw = |i: usize| toks.get(i).and_then(Tok::word);
    match kw(1) {
        Some("NAMES") => {
            info.effects.push(Effect::SetNames);
            return;
        }
        Some("CHARACTER" | "CHARSET" | "GLOBAL") => return,
        Some("TRANSACTION") => {
            info.effects.push(Effect::NextTransaction);
            return;
        }
        Some("ROLE" | "PASSWORD" | "DEFAULT") => {
            if kw(1) == Some("ROLE") {
                info.effects.push(Effect::Untracked("SET ROLE".into()));
            }
            return;
        }
        Some("SESSION" | "LOCAL") if kw(2) == Some("TRANSACTION") => return,
        _ => {}
    }
    // Walk the comma-separated assignments at depth 0.
    let mut depth = 0i32;
    let mut at_start = true;
    let mut i = 1;
    while i < toks.len() {
        match &toks[i] {
            Tok::Punct(p) if p == "(" => depth += 1,
            Tok::Punct(p) if p == ")" => depth -= 1,
            Tok::Punct(p) if p == "," && depth == 0 => {
                at_start = true;
                i += 1;
                continue;
            }
            _ => {}
        }
        if at_start && depth == 0 {
            at_start = false;
            match &toks[i] {
                Tok::UserVar(v) => {
                    info.reads_user_vars.retain(|r| r != v);
                    push_unique(&mut info.assigns_user_vars, v);
                }
                Tok::SysVar(scope, name) => {
                    if scope.as_deref() == Some("GLOBAL") {
                        // Server-wide: the server decides (1227 without privilege).
                    } else {
                        note_session_var(name, info);
                    }
                }
                Tok::Word(w) if w == "SESSION" || w == "LOCAL" => {
                    if let Some(name) = toks.get(i + 1).and_then(Tok::ident) {
                        note_session_var(&name, info);
                        i += 1;
                    }
                }
                Tok::Word(w) if w == "GLOBAL" => {
                    i += 1;
                }
                t => {
                    if let Some(name) = t.ident() {
                        note_session_var(&name, info);
                    }
                }
            }
        }
        i += 1;
    }
    // A user variable read on the right-hand side of its own assignment
    // still counts as read.
}

fn note_session_var(name: &str, info: &mut StmtInfo) {
    let lower = name.to_ascii_lowercase();
    if super::state::gateway_var(&lower) {
        push_unique(&mut info.sets_gateway_vars, &lower);
    } else if super::state::var_kind(&lower).is_none() {
        info.effects.push(Effect::Untracked(format!("SET {lower}")));
    }
}

fn push_unique(v: &mut Vec<String>, s: &str) {
    if !v.iter().any(|x| x == s) {
        v.push(s.to_owned());
    }
}

/// The identifier after the first `kw`, skipping `IF NOT EXISTS`.
fn name_after(toks: &[Tok], kw: &str) -> Option<String> {
    let i = toks.iter().position(|t| t.word() == Some(kw))?;
    let mut j = i + 1;
    while matches!(
        toks.get(j).and_then(Tok::word),
        Some("IF" | "NOT" | "EXISTS")
    ) {
        j += 1;
    }
    qualified_name(toks, j)
}

/// `a`, `db.a` or `` `db`.`a` ``: the table name, lower case, without the
/// schema.
fn qualified_name(toks: &[Tok], j: usize) -> Option<String> {
    let mut name = toks.get(j)?.ident()?;
    if matches!(toks.get(j + 1), Some(Tok::Punct(p)) if p == ".") {
        name = toks.get(j + 2)?.ident()?;
    }
    Some(name.to_ascii_lowercase())
}

/// The tables of `DROP [TEMPORARY] TABLE [IF EXISTS] a, b`.
fn table_list(toks: &[Tok]) -> Vec<String> {
    let Some(i) = toks
        .iter()
        .position(|t| matches!(t.word(), Some("TABLE" | "TABLES")))
    else {
        return Vec::new();
    };
    let mut j = i + 1;
    while matches!(toks.get(j).and_then(Tok::word), Some("IF" | "EXISTS")) {
        j += 1;
    }
    let mut out = Vec::new();
    while let Some(n) = qualified_name(toks, j) {
        out.push(n);
        j += if matches!(toks.get(j + 1), Some(Tok::Punct(p)) if p == ".") {
            3
        } else {
            1
        };
        if matches!(toks.get(j), Some(Tok::Punct(p)) if p == ",") {
            j += 1;
        } else {
            break;
        }
    }
    out
}

/// A token of the scan.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    /// A keyword or bare identifier, upper case.
    Word(String),
    /// A back-quoted identifier, as written.
    Quoted(String),
    /// `@name`, lower case.
    UserVar(String),
    /// `@@[scope.]name`: scope upper case, name lower case.
    SysVar(Option<String>, String),
    /// A string or number literal.
    Literal,
    /// Punctuation; `:=` is one token.
    Punct(String),
}

impl Tok {
    fn word(&self) -> Option<&str> {
        match self {
            Tok::Word(w) => Some(w),
            _ => None,
        }
    }

    fn ident(&self) -> Option<String> {
        match self {
            Tok::Word(w) => Some(w.to_ascii_lowercase()),
            Tok::Quoted(q) => Some(q.clone()),
            _ => None,
        }
    }
}

/// Splits statement text into tokens, skipping comments (except
/// executable `/*! */` and `/*M! */` comments, whose contents are scanned).
#[allow(clippy::too_many_lines)]
#[allow(clippy::many_single_char_names)] // a lexer's usual i, n, c, q, s
fn tokenize(sql: &[u8]) -> Vec<Tok> {
    let mut out = Vec::new();
    let mut i = 0;
    let n = sql.len();
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80;
    while i < n {
        let c = sql[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'#' => {
                while i < n && sql[i] != b'\n' {
                    i += 1;
                }
            }
            b'-' if sql.get(i + 1) == Some(&b'-')
                && sql.get(i + 2).is_none_or(u8::is_ascii_whitespace) =>
            {
                while i < n && sql[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if sql.get(i + 1) == Some(&b'*') => {
                let exec = sql.get(i + 2) == Some(&b'!')
                    || (sql.get(i + 2) == Some(&b'M') && sql.get(i + 3) == Some(&b'!'));
                if exec {
                    // Scan the inside; skip the version digits.
                    i += if sql.get(i + 2) == Some(&b'M') { 4 } else { 3 };
                    while i < n && sql[i].is_ascii_digit() {
                        i += 1;
                    }
                } else {
                    i += 2;
                    while i + 1 < n && !(sql[i] == b'*' && sql[i + 1] == b'/') {
                        i += 1;
                    }
                    i = (i + 2).min(n);
                }
            }
            b'*' if sql.get(i + 1) == Some(&b'/') => i += 2,
            b'\'' | b'"' => {
                let q = c;
                i += 1;
                while i < n {
                    if sql[i] == b'\\' {
                        i += 2;
                    } else if sql[i] == q {
                        if sql.get(i + 1) == Some(&q) {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                out.push(Tok::Literal);
            }
            b'`' => {
                let mut s = Vec::new();
                i += 1;
                while i < n {
                    if sql[i] == b'`' {
                        if sql.get(i + 1) == Some(&b'`') {
                            s.push(b'`');
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        s.push(sql[i]);
                        i += 1;
                    }
                }
                out.push(Tok::Quoted(String::from_utf8_lossy(&s).into_owned()));
            }
            b'@' if sql.get(i + 1) == Some(&b'@') => {
                i += 2;
                let start = i;
                while i < n && (is_word(sql[i]) || sql[i] == b'.') {
                    i += 1;
                }
                let text = String::from_utf8_lossy(&sql[start..i]).into_owned();
                let (scope, name) = match text.split_once('.') {
                    Some((s, v)) => (Some(s.to_ascii_uppercase()), v.to_ascii_lowercase()),
                    None => (None, text.to_ascii_lowercase()),
                };
                out.push(Tok::SysVar(scope, name));
            }
            b'@' => {
                i += 1;
                let name = if matches!(sql.get(i), Some(b'`' | b'\'' | b'"')) {
                    let q = sql[i];
                    i += 1;
                    let start = i;
                    while i < n && sql[i] != q {
                        i += 1;
                    }
                    let s = String::from_utf8_lossy(&sql[start..i.min(n)]).into_owned();
                    i += 1;
                    s
                } else {
                    let start = i;
                    while i < n && (is_word(sql[i]) || sql[i] == b'.') {
                        i += 1;
                    }
                    String::from_utf8_lossy(&sql[start..i]).into_owned()
                };
                out.push(Tok::UserVar(name.to_ascii_lowercase()));
            }
            b':' if sql.get(i + 1) == Some(&b'=') => {
                out.push(Tok::Punct(":=".into()));
                i += 2;
            }
            c if c.is_ascii_digit() => {
                while i < n && (is_word(sql[i]) || sql[i] == b'.') {
                    i += 1;
                }
                out.push(Tok::Literal);
            }
            c if is_word(c) => {
                let start = i;
                while i < n && is_word(sql[i]) {
                    i += 1;
                }
                out.push(Tok::Word(
                    String::from_utf8_lossy(&sql[start..i]).to_ascii_uppercase(),
                ));
            }
            _ => {
                out.push(Tok::Punct((c as char).to_string()));
                i += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_arguments_are_captured_as_assignments() {
        let i = StmtInfo::classify(b"CALL p(1, @b, @c)");
        assert!(i.reads_all_user_vars);
        assert_eq!(i.assigns_user_vars, ["b", "c"]);
    }

    fn c(s: &str) -> StmtInfo {
        StmtInfo::classify(s.as_bytes())
    }

    #[test]
    fn plain_reads() {
        for s in [
            "SELECT * FROM t WHERE id = 1",
            "  /* hint */ select a from t",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "SELECT @a, LAST_INSERT_ID()",
            "EXPLAIN SELECT 1",
            "SELECT 'FOR UPDATE' AS s",
        ] {
            assert_eq!(c(s).class, StmtClass::Read, "{s}");
        }
        let r = c("SELECT @a, LAST_INSERT_ID()");
        assert_eq!(r.reads_user_vars, vec!["a"]);
        assert!(r.uses_last_insert_id);
    }

    #[test]
    fn reads_that_must_stay_on_the_writer() {
        for s in [
            "SELECT * FROM t FOR UPDATE",
            "SELECT * FROM t LOCK IN SHARE MODE",
            "SELECT * FROM t FOR SHARE",
            "SELECT a INTO @x FROM t",
            "SELECT @x := 1",
            "SELECT GET_LOCK('a', 1)",
            "SELECT NEXTVAL(s)",
            "SELECT LAST_INSERT_ID(5)",
            "SELECT FOUND_ROWS()",
            "SHOW PROCESSLIST",
            "INSERT INTO t VALUES (1)",
            "CALL p()",
        ] {
            assert_eq!(c(s).class, StmtClass::Write, "{s}");
        }
        assert_eq!(c("SELECT a INTO @x FROM t").assigns_user_vars, vec!["x"]);
        assert_eq!(c("SELECT @x := 1").assigns_user_vars, vec!["x"]);
        assert_eq!(c("SELECT GET_LOCK('a', 1)").effects, vec![Effect::GetLock]);
    }

    #[test]
    fn set_statements() {
        let s = c("SET @a = 1, @b = @a + 1");
        assert_eq!(s.assigns_user_vars, vec!["a", "b"]);
        assert!(s.effects.is_empty());
        assert!(c("SET SESSION sql_mode = 'ANSI'").effects.is_empty());
        assert!(
            c("SET @@session.time_zone = '+00:00', autocommit = 0")
                .effects
                .is_empty()
        );
        assert_eq!(c("SET NAMES utf8mb4").effects, vec![Effect::SetNames]);
        assert!(c("SET NAMES utf8mb4 COLLATE utf8mb4_bin").sets_names());
        assert!(!c("SET CHARACTER SET utf8mb4").sets_names());
        assert!(!c("SET character_set_client = utf8mb4").sets_names());
        assert_eq!(
            c("SET timestamp = 1").effects,
            vec![Effect::Untracked("SET timestamp".into())]
        );
        assert_eq!(
            c("SET @@session.sql_log_off = 1").effects,
            vec![Effect::Untracked("SET sql_log_off".into())]
        );
        assert_eq!(
            c("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").effects,
            vec![Effect::NextTransaction]
        );
        assert!(
            c("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .effects
                .is_empty()
        );
        assert!(c("SET GLOBAL max_connections = 10").effects.is_empty());
        let w = c("SET wait_timeout = 10");
        assert!(w.effects.is_empty());
        assert_eq!(w.sets_gateway_vars, ["wait_timeout"]);
        // MariaDB Connector/J's start-up statement.
        let j = c("set autocommit=1, session_track_schema=1, \
                   session_track_system_variables = CONCAT(@@global.session_track_system_variables, ',auto_increment_increment')");
        assert!(j.effects.is_empty());
        assert_eq!(
            j.sets_gateway_vars,
            ["session_track_schema", "session_track_system_variables"]
        );
        assert!(j.expects_state_change());
        assert!(
            c("SET GLOBAL wait_timeout = 10")
                .sets_gateway_vars
                .is_empty()
        );
    }

    #[test]
    fn set_statement_scopes_its_variables_to_the_inner_statement() {
        let r =
            c("SET STATEMENT max_statement_time = 1, @@session.sql_mode = @m FOR SELECT * FROM t");
        assert_eq!(r.class, StmtClass::Read, "the inner statement decides");
        assert_eq!(r.scoped_vars, ["max_statement_time", "sql_mode"]);
        assert_eq!(r.reads_user_vars, ["m"]);
        assert!(r.effects.is_empty());
        assert!(r.expects_state_change());
        let w = c("SET STATEMENT innodb_lock_wait_timeout = 1 FOR SELECT * FROM t FOR UPDATE");
        assert_eq!(w.class, StmtClass::Write);
        assert_eq!(w.scoped_vars, ["innodb_lock_wait_timeout"]);
        assert!(
            w.effects.is_empty(),
            "no Untracked pin for a scoped variable"
        );
        let f =
            c("SET STATEMENT sql_mode = (SELECT 'ANSI' FROM dual) FOR INSERT INTO t VALUES (1)");
        assert_eq!(f.class, StmtClass::Write);
        assert_eq!(f.scoped_vars, ["sql_mode"]);
        assert_eq!(c("SET STATEMENT x = 1").class, StmtClass::Write);
    }

    #[test]
    fn transactions_temp_tables_and_locks() {
        assert_eq!(c("begin").effects, vec![Effect::Begin]);
        assert_eq!(
            c("START TRANSACTION READ ONLY").effects,
            vec![Effect::Begin]
        );
        assert_eq!(c("COMMIT").effects, vec![Effect::Commit]);
        assert!(c("COMMIT").commits());
        assert_eq!(c("ROLLBACK").effects, vec![Effect::Rollback]);
        assert!(c("ROLLBACK TO SAVEPOINT a").effects.is_empty());
        assert_eq!(
            c("CREATE TEMPORARY TABLE IF NOT EXISTS `Tmp` (a int)").effects,
            vec![Effect::CreateTemp("tmp".into())]
        );
        assert_eq!(
            c("DROP TEMPORARY TABLE IF EXISTS tmp, db.other").effects,
            vec![Effect::DropTables(vec!["tmp".into(), "other".into()])]
        );
        assert_eq!(
            c("DROP TABLE t").effects,
            vec![Effect::DropTables(vec!["t".into()]), Effect::ImplicitCommit]
        );
        assert_eq!(
            c("CREATE TABLE t (a int)").effects,
            vec![Effect::ImplicitCommit]
        );
        assert_eq!(c("LOCK TABLES t WRITE").effects, vec![Effect::LockTables]);
        assert_eq!(c("UNLOCK TABLES").effects, vec![Effect::UnlockTables]);
        assert_eq!(c("DO RELEASE_LOCK('a')").effects, vec![Effect::ReleaseLock]);
        assert_eq!(c("HANDLER t OPEN").effects, vec![Effect::HandlerOpen]);
        assert_eq!(c("XA START 'x'").effects, vec![Effect::XaStart]);
        assert!(matches!(
            c("PREPARE s FROM 'select 1'").effects[0],
            Effect::Untracked(_)
        ));
        assert!(matches!(
            c("SELECT LASTVAL(s)").effects[0],
            Effect::Untracked(_)
        ));
        let call = c("CALL p(@a)");
        assert!(call.reads_all_user_vars);
        assert_eq!(call.effects, vec![Effect::Call]);
        assert!(c("SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 1").calc_found_rows);
    }

    #[test]
    fn comments_and_strings_hide_keywords() {
        assert_eq!(
            c("SELECT '@x := 1', \"GET_LOCK(\" -- FOR UPDATE\n").class,
            StmtClass::Read
        );
        assert_eq!(c("SELECT 1 /*!50000 FOR UPDATE */").class, StmtClass::Write);
        assert_eq!(c("# c\nSELECT 1").class, StmtClass::Read);
    }
}
