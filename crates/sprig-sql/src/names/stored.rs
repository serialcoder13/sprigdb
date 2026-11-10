//! Account functions and `DATABASE()` inside stored programs and views
//! (chapter 07, "Account names"; build step 12, R4).
//!
//! A view, procedure, function, trigger or event runs inside MariaDB, as
//! the tenant's backend account and in a physical schema, where the gateway
//! never sees its values. Left alone, `CURRENT_USER()` inside one returns
//! `t42_u7@%` and `DATABASE()` returns `t42_d1_g1_s1`, both into results
//! and into stored rows. So at `CREATE` (and `ALTER`) time, [`substitute`]
//! replaces each call in the body with a lookup that computes the logical
//! value inside MariaDB:
//!
//! | Call | Becomes |
//! |---|---|
//! | `DATABASE()`, `SCHEMA()` | `` (SELECT `logical` FROM <names> WHERE `name` = DATABASE()) `` |
//! | `CURRENT_USER`, `CURRENT_USER()` | `` (SELECT `account` FROM <names> WHERE `name` = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)) `` |
//! | `USER()`, `SESSION_USER()`, `SYSTEM_USER()` | `` (SELECT CONCAT(`logical`, '@', SUBSTRING_INDEX(USER(), '@', -1)) FROM <names> WHERE `name` = SUBSTRING_INDEX(USER(), '@', 1)) `` |
//!
//! `<names>` is the per-cluster view `` `__sprig_sys`.`__sprig_names_v` ``
//! over the table the gateway keeps ([`setup_sql`], [`NameRow`]): one row
//! per backend account (`t42_u7` to `alice`, `alice@%`) and per physical
//! schema (`t42_d1_g1_s1` to `shop`) of every tenant on the cluster. The
//! view shows a session only its own tenant's rows (the tenant prefix of
//! `SESSION_USER()`, which MariaDB sets at login and no SQL can change), and
//! is granted to `PUBLIC`, so every backend account can read it without a
//! grant of its own, and no tenant can read another's names, even from
//! dynamic SQL inside its own stored code.
//!
//! The lookup is evaluated where MySQL evaluates the function, so the value
//! is MySQL's: a view's `DATABASE()` is the session's current database, a
//! routine's its own, `CURRENT_USER()` is the definer in `SQL SECURITY
//! DEFINER` code and the invoker otherwise. The host of `USER()` is the
//! address MariaDB sees (the gateway's), not the client's.
//!
//! A call that is a whole select item without an alias gets one, the call's
//! own text (`` AS `CURRENT_USER()` ``), so the column keeps MySQL's name.
//! [`restore`] turns each lookup back into the call wherever MariaDB shows
//! a body (`SHOW CREATE`, `SHOW TRIGGERS`, `information_schema`
//! definitions), in MariaDB's verbatim routine text and in its canonical
//! view text alike.
//!
//! **Caught error text.** An error a handler catches holds physical names
//! (`Table 't42_d1_g1_s1.x' doesn't exist`). So each `GET [CURRENT |
//! STACKED] DIAGNOSTICS` in a body is followed by a mapping of every
//! target that receives a text item (`MESSAGE_TEXT`, `SCHEMA_NAME`,
//! `TABLE_NAME`, ...; not `MYSQL_ERRNO`, `RETURNED_SQLSTATE` and the other
//! numbers), through [`MAP_FUNCTION`], a function on each cluster that
//! maps names in text exactly as the gateway maps an error message
//! ([`super::output::map_message`]):
//!
//! ```text
//! GET DIAGNOSTICS CONDITION 1 m = MESSAGE_TEXT
//! BEGIN GET DIAGNOSTICS CONDITION 1 m = MESSAGE_TEXT; SET m = `__sprig_sys`.`__sprig_names_map`(m); END
//! ```
//!
//! The block makes one statement of the pair wherever a statement stands
//! (a handler's body, a branch). It carries no comment, so it also works
//! inside an executable comment (`mysqldump` writes triggers in
//! `/*!50003 ... */`); [`restore`] recognizes it by the `SET`s after the
//! statement, which no client can write (`__sprig_sys` is reserved).
//! Neither the block nor the `SET` clears the diagnostics area (measured
//! on MariaDB 11.8: a later `GET DIAGNOSTICS`, `RESIGNAL` and `SHOW
//! WARNINGS` see the same conditions).
//! MariaDB's `SQLERRM` (`sql_mode=ORACLE`) becomes
//! `` `__sprig_sys`.`__sprig_names_map`(SQLERRM) ``. [`top_level`] does
//! the same for a client's own `GET DIAGNOSTICS`, compound statements and
//! the text of `PREPARE ... FROM '...'`, and [`restore`] removes all of it
//! from bodies MariaDB shows.

use std::borrow::Cow;
use std::ops::Range;

use crate::lex::{LexOptions, Token, TokenKind, lex};

/// The per-cluster system schema (writing guide, "Per-cluster system
/// schema").
pub const SYS_SCHEMA: &str = "__sprig_sys";
/// The table of name pairs, in [`SYS_SCHEMA`].
pub const NAMES_TABLE: &str = "__sprig_names";
/// The tenant-filtered view over [`NAMES_TABLE`], granted to `PUBLIC`.
pub const NAMES_VIEW: &str = "__sprig_names_v";
/// The function that maps physical names in text (caught error text), in
/// [`SYS_SCHEMA`]; every account may run it (see [`setup_sql`]).
pub const MAP_FUNCTION: &str = "__sprig_names_map";
const VIEW_REF: &str = "`__sprig_sys`.`__sprig_names_v`";
const MAP_REF: &str = "`__sprig_sys`.`__sprig_names_map`";
const MARKER: &[u8] = b"__sprig_names_v";
/// What every piece of SprigDB's text in a body contains.
const ANY_MARKER: &[u8] = b"__sprig_names";
/// The block around a `GET DIAGNOSTICS` in a body: its start, each
/// mapping after the statement, and its end.
const GD_OPEN: &str = "BEGIN ";
const GD_SET: &str = "; SET ";
const GD_CLOSE: &str = "; END";

/// [`MAP_FUNCTION`]'s definition. It maps each identifier run of `s`
/// (`[A-Za-z0-9_$]` and non-ASCII) the way [`super::output::map_message`]
/// does: the session tenant's schema (any group, shard or `_ref` of a live
/// database) becomes the database's name, its user account the user's
/// name, any other physical name `?`, `Tables_in_<schema>` the same with
/// the name mapped, and `__sprig_lk_<t>__...`, `__sprig_ix_<t>__...` and
/// `__sprig_wx_<t>` their table. Inside backquotes a backquote in the
/// replacement is doubled. Text without a candidate run comes back as it
/// is. It returns MariaDB's type for diagnostics items (`utf8mb3`,
/// `utf8mb3_general_ci`), so a user variable keeps its collation. The
/// names come from [`NAMES_VIEW`], so a session sees only its own
/// tenant's; `SQL SECURITY INVOKER`, so it runs with the caller's rights.
/// Regular expressions are case-sensitive (`(?-i)`) whatever the
/// collation, and hold no backslash, so no `sql_mode` changes them.
fn map_function_sql() -> String {
    const RUN: &str = "[A-Za-z0-9_$[:^ascii:]]";
    const N20: &str = "(0|[1-9][0-9]{0,19})";
    const N10: &str = "(0|[1-9][0-9]{0,9})";
    format!(
        "CREATE OR REPLACE FUNCTION `{SYS_SCHEMA}`.`{MAP_FUNCTION}`(s TEXT CHARSET utf8mb4) \
         RETURNS TEXT CHARSET utf8mb3 COLLATE utf8mb3_general_ci \
         READS SQL DATA SQL SECURITY INVOKER \
         COMMENT 'SprigDB: physical names in text as the tenant knows them' \
         BEGIN \
         DECLARE o, r, w, x, q TEXT CHARSET utf8mb4 COLLATE utf8mb4_bin DEFAULT ''; \
         DECLARE p VARCHAR(10) CHARSET utf8mb4 COLLATE utf8mb4_bin DEFAULT ''; \
         DECLARE i INT; \
         IF s IS NULL OR NOT s REGEXP '(?-i)t[0-9]|Tables_in_t|__sprig_' THEN RETURN s; END IF; \
         SET r = s; \
         scan: LOOP \
         SET i = REGEXP_INSTR(r, '(?-i)(?<!{RUN})(t[0-9]|Tables_in_t|__sprig_){RUN}*'); \
         IF i = 0 THEN LEAVE scan; END IF; \
         SET w = REGEXP_SUBSTR(SUBSTRING(r, i), '^{RUN}+'); \
         SET o = CONCAT(o, LEFT(r, i - 1)); \
         SET r = SUBSTRING(r, i + CHAR_LENGTH(w)); \
         SET x = NULL, p = '', q = w; \
         IF LEFT(w, 10) = 'Tables_in_' THEN SET p = 'Tables_in_', q = SUBSTRING(w, 11); END IF; \
         IF q REGEXP '(?-i)^t{N20}_d{N10}_(ref|g{N10}_s{N10})$' THEN \
         SET q = SUBSTRING_INDEX(q, '_', 2); \
         SET x = CONCAT(p, IFNULL((SELECT `logical` FROM {VIEW_REF} WHERE `account` IS NULL \
         AND LEFT(`name`, CHAR_LENGTH(q) + 1) = CONCAT(q, '_') LIMIT 1), '?')); \
         ELSEIF q REGEXP '(?-i)^t{N20}_(sprig|rc[0-9a-f]{{8}}|r{N10})$' THEN \
         SET x = CONCAT(p, '?'); \
         ELSEIF q REGEXP '(?-i)^t{N20}_u{N10}$' THEN \
         SET x = CONCAT(p, IFNULL((SELECT `logical` FROM {VIEW_REF} WHERE `account` IS NOT NULL \
         AND `name` = q), '?')); \
         ELSEIF p = '' AND w REGEXP '(?-i)^__sprig_(lk|ix)_' THEN \
         SET q = SUBSTRING(w, 12); \
         IF LOCATE('__', q) > 1 THEN SET x = SUBSTRING_INDEX(q, '__', 1); END IF; \
         ELSEIF p = '' AND w REGEXP '(?-i)^__sprig_wx_.' THEN \
         SET x = SUBSTRING(w, 12); \
         END IF; \
         IF x IS NULL THEN SET o = CONCAT(o, w); \
         ELSEIF RIGHT(o, 1) = '`' AND LEFT(r, 1) = '`' THEN SET o = CONCAT(o, REPLACE(x, '`', '``')); \
         ELSE SET o = CONCAT(o, x); \
         END IF; \
         END LOOP; \
         RETURN CONCAT(o, r); \
         END"
    )
}

/// The statements that create the lookup objects on a cluster, as
/// `sprig_ctl` (idempotent; the grants to `PUBLIC` need MariaDB 10.11).
/// MariaDB 11.8 ignores a routine-level grant to `PUBLIC` (measured: 1044),
/// so [`MAP_FUNCTION`] is granted with `EXECUTE` on the whole
/// [`SYS_SCHEMA`]: no other routine may live there.
#[must_use]
pub fn setup_sql() -> [String; 5] {
    [
        format!(
            "CREATE TABLE IF NOT EXISTS `{SYS_SCHEMA}`.`{NAMES_TABLE}` (\
             `name` VARCHAR(80) NOT NULL PRIMARY KEY, \
             `logical` VARCHAR(255) NOT NULL, \
             `account` VARCHAR(400) NULL\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        ),
        format!(
            "CREATE OR REPLACE SQL SECURITY DEFINER VIEW `{SYS_SCHEMA}`.`{NAMES_VIEW}` AS \
             SELECT `name`, `logical`, `account` FROM `{SYS_SCHEMA}`.`{NAMES_TABLE}` \
             WHERE LEFT(`name`, LOCATE('_', `name`)) = LEFT(SESSION_USER(), LOCATE('_', SESSION_USER()))"
        ),
        format!("GRANT SELECT ON `{SYS_SCHEMA}`.`{NAMES_VIEW}` TO PUBLIC"),
        map_function_sql(),
        format!("GRANT EXECUTE ON `{SYS_SCHEMA}`.* TO PUBLIC"),
    ]
}

/// One row of [`NAMES_TABLE`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NameRow {
    /// A backend user (`t42_u7`) or a physical schema (`t42_d1_g1_s1`).
    pub name: String,
    /// The user name or the logical database name.
    pub logical: String,
    /// For an account, `user@host` as `CURRENT_USER()` shows it.
    pub account: Option<String>,
}

/// Which call a lookup stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    Database,
    CurrentUser,
    User,
    /// `VERSION()`: the persona's version string (gaps row 53).
    Version,
}

fn call_of(word: &[u8]) -> Option<Call> {
    [
        (&b"DATABASE"[..], Call::Database),
        (b"SCHEMA", Call::Database),
        (b"CURRENT_USER", Call::CurrentUser),
        (b"USER", Call::User),
        (b"SESSION_USER", Call::User),
        (b"SYSTEM_USER", Call::User),
        (b"VERSION", Call::Version),
    ]
    .into_iter()
    .find(|(n, _)| n.eq_ignore_ascii_case(word))
    .map(|(_, c)| c)
}

/// `VERSION()` in a body: the version the creating session's persona
/// reports, as a constant inside a group [`restore`] recognizes. The
/// aggregate gives one row whatever the view holds, and the `WHERE` is
/// never true, so nothing is read.
fn version_lookup(func: &str, version: &str, nbe: bool) -> String {
    let v = super::rewrite::sql_string(version, nbe);
    format!("(SELECT COALESCE(MAX(NULL), {v}) FROM {VIEW_REF} WHERE {func}() IS NULL)")
}

fn lookup(call: Call, func: &str) -> String {
    match call {
        Call::Version => unreachable!("version_lookup"),
        Call::Database => format!("(SELECT `logical` FROM {VIEW_REF} WHERE `name` = {func}())"),
        Call::CurrentUser => format!(
            "(SELECT `account` FROM {VIEW_REF} WHERE `name` = SUBSTRING_INDEX({func}(), '@', 1))"
        ),
        Call::User => format!(
            "(SELECT CONCAT(`logical`, '@', SUBSTRING_INDEX({func}(), '@', -1)) FROM {VIEW_REF} \
             WHERE `name` = SUBSTRING_INDEX({func}(), '@', 1))"
        ),
    }
}

/// Words after which `CURRENT_USER` names an account, not a value
/// (`DEFINER =`, `GRANT ... TO`, `SET PASSWORD FOR`, `ALTER USER`, ...).
const ACCOUNT_CONTEXT: &[&[u8]] = &[b"FOR", b"TO", b"FROM", b"USER", b"DEFINER"];

/// Words that end a select item.
const ITEM_END: &[&[u8]] = &[
    b"FROM",
    b"INTO",
    b"UNION",
    b"EXCEPT",
    b"INTERSECT",
    b"LIMIT",
    b"ORDER",
    b"GROUP",
    b"HAVING",
    b"WHERE",
    b"WINDOW",
    b"FOR",
    b"LOCK",
];

/// Words that start a select list item list (after `SELECT`).
const SELECT_MODIFIERS: &[&[u8]] = &[
    b"SELECT",
    b"DISTINCT",
    b"ALL",
    b"DISTINCTROW",
    b"HIGH_PRIORITY",
    b"STRAIGHT_JOIN",
    b"SQL_SMALL_RESULT",
    b"SQL_BIG_RESULT",
    b"SQL_BUFFER_RESULT",
    b"SQL_NO_CACHE",
    b"SQL_CACHE",
    b"SQL_CALC_FOUND_ROWS",
];

/// `sql` (the physical text of a `CREATE` or `ALTER` of a view, routine,
/// trigger or event) with every account function and `DATABASE()` in it
/// replaced by its lookup (see the module docs), or `None` when it holds
/// none. `DEFINER` clauses and account statements inside bodies keep
/// `CURRENT_USER`; string literals (dynamic SQL) are not looked into;
/// lookups already in the text are left alone, so a re-creation from
/// `SHOW CREATE` text doesn't nest them.
///
/// Each `GET DIAGNOSTICS` gets its mapping (see the module docs); `SQLERRM`
/// only through [`substitute_with`] with `oracle`.
#[must_use]
pub fn substitute(sql: &str, lex_opts: &LexOptions) -> Option<String> {
    substitute_with(sql, lex_opts, false)
}

/// [`substitute`], with MariaDB's `SQLERRM` mapped too when the body is
/// created under `sql_mode=ORACLE` (`oracle`), where it is the caught
/// error's text (elsewhere it is an ordinary name).
#[must_use]
pub fn substitute_with(sql: &str, lex_opts: &LexOptions, oracle: bool) -> Option<String> {
    substitute_full(sql, lex_opts, oracle, None)
}

/// [`substitute_with`], with `VERSION()` also replaced by `version`, the
/// version string the creating session's persona reports (gaps row 53:
/// MariaDB would answer its own, `11.8.9-MariaDB-...`, on both ports).
/// The value is fixed when the program is created; [`restore`] shows the
/// call again.
#[must_use]
#[allow(clippy::too_many_lines)] // one pass over the tokens
pub fn substitute_full(
    sql: &str,
    lex_opts: &LexOptions,
    oracle: bool,
    version: Option<&str>,
) -> Option<String> {
    let src = sql.as_bytes();
    let opts = lex_opts.version(999_999);
    let toks: Vec<Token> = lex(src, &opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let ours = groups(src);
    let inside_ours = |t: &Token| {
        ours.iter()
            .any(|g| g.start <= t.start as usize && (t.end as usize) <= g.end)
    };
    let word = |i: usize, w: &[u8]| {
        toks.get(i)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(src).eq_ignore_ascii_case(w))
    };
    let punct = |i: usize, p: &[u8]| {
        toks.get(i).is_some_and(|t| {
            matches!(t.kind, TokenKind::Punct | TokenKind::Op) && t.bytes(src) == p
        })
    };
    // Paren depth, and at each depth whether a select list is open.
    let mut depth = 0usize;
    let mut select_list: Vec<bool> = vec![false];
    let mut edits: Vec<(Range<usize>, String)> = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        if punct(i, b"(") {
            depth += 1;
            select_list.push(false);
            i += 1;
            continue;
        }
        if punct(i, b")") {
            depth = depth.saturating_sub(1);
            if select_list.len() > 1 {
                select_list.pop();
            }
            i += 1;
            continue;
        }
        if punct(i, b";") {
            if let Some(s) = select_list.last_mut() {
                *s = false;
            }
            i += 1;
            continue;
        }
        if t.kind != TokenKind::Word {
            i += 1;
            continue;
        }
        let b = t.bytes(src);
        if b.eq_ignore_ascii_case(b"SELECT") {
            if let Some(s) = select_list.last_mut() {
                *s = true;
            }
            i += 1;
            continue;
        }
        if ITEM_END.iter().any(|w| b.eq_ignore_ascii_case(w))
            && let Some(s) = select_list.last_mut()
        {
            *s = false;
        }
        let Some(call) = call_of(b).filter(|c| *c != Call::Version || version.is_some()) else {
            i += 1;
            continue;
        };
        let parens = punct(i + 1, b"(") && punct(i + 2, b")");
        let bare_ok = call == Call::CurrentUser;
        let qualified = i > 0 && punct(i - 1, b".");
        let account_ctx = call == Call::CurrentUser
            && i > 0
            && (ACCOUNT_CONTEXT.iter().any(|w| word(i - 1, w))
                || (punct(i - 1, b"=") && i > 1 && word(i - 2, b"DEFINER")));
        // `ALTER USER USER() ...`, `SET PASSWORD FOR USER()`.
        let account_user = call == Call::User
            && i > 0
            && (word(i - 1, b"USER") || word(i - 1, b"FOR") || word(i - 1, b"TO"));
        if (!parens && !bare_ok) || qualified || account_ctx || account_user || inside_ours(&t) {
            i += 1;
            continue;
        }
        let last = if parens { i + 2 } else { i };
        let func = String::from_utf8_lossy(b).into_owned();
        let mut rep = match (call, version) {
            (Call::Version, Some(v)) => version_lookup(&func, v, lex_opts.no_backslash_escapes),
            _ => lookup(call, &func),
        };
        // A whole select item without an alias keeps the call's name.
        let in_list = select_list.last().copied().unwrap_or(false);
        let starts_item =
            i > 0 && (punct(i - 1, b",") || SELECT_MODIFIERS.iter().any(|w| word(i - 1, w)));
        let n = last + 1;
        let ends_item = n >= toks.len()
            || punct(n, b",")
            || punct(n, b";")
            || punct(n, b")")
            || ITEM_END.iter().any(|w| word(n, w));
        if in_list && starts_item && ends_item {
            let text = String::from_utf8_lossy(&src[t.start as usize..toks[last].end as usize]);
            rep.push_str(" AS `");
            rep.push_str(&text.replace('`', "``"));
            rep.push('`');
        }
        edits.push((t.start as usize..toks[last].end as usize, rep));
        i = last + 1;
    }
    edits.extend(diagnostics_edits(src, &toks, oracle));
    let out = apply(src, edits)?;
    // The edits insert ASCII at token boundaries of valid UTF-8.
    String::from_utf8(out).ok()
}

/// `src` with `edits` (ranges in `src`, an empty range an insertion)
/// applied; `None` when there are none.
fn apply(src: &[u8], mut edits: Vec<(Range<usize>, String)>) -> Option<Vec<u8>> {
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|(r, _)| (r.start, r.end));
    let mut out = Vec::with_capacity(src.len() + edits.len() * 120);
    let mut copied = 0;
    for (r, rep) in edits {
        if r.start < copied {
            // Overlapping edits can't happen; never write a broken text.
            return None;
        }
        out.extend_from_slice(&src[copied..r.start]);
        out.extend_from_slice(rep.as_bytes());
        copied = r.end;
    }
    out.extend_from_slice(&src[copied..]);
    Some(out)
}

/// Diagnostics items that are numbers or an SQLSTATE: never a name, and
/// a mapping would turn a user variable's number into a string.
const NUMERIC_ITEMS: &[&[u8]] = &[
    b"NUMBER",
    b"ROW_COUNT",
    b"MYSQL_ERRNO",
    b"RETURNED_SQLSTATE",
    b"ERROR_INDEX",
    b"ROW_NUMBER",
];

/// One `GET DIAGNOSTICS` statement found in a token list.
struct Diagnostics {
    /// The index of its last token.
    last: usize,
    /// The targets of text items, as written (`m`, `@m`, `` `m` ``).
    targets: Vec<String>,
}

/// The `GET [CURRENT | STACKED] DIAGNOSTICS` statement starting at token
/// `i`, if it is one and has text items. A statement whose shape isn't
/// understood (MariaDB would refuse it) gives `None`.
fn diagnostics_at(src: &[u8], toks: &[Token], i: usize) -> Option<Diagnostics> {
    let word = |j: usize, w: &[u8]| {
        toks.get(j)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(src).eq_ignore_ascii_case(w))
    };
    let punct = |j: usize, p: &[u8]| {
        toks.get(j).is_some_and(|t| {
            matches!(t.kind, TokenKind::Punct | TokenKind::Op) && t.bytes(src) == p
        })
    };
    if !word(i, b"GET") {
        return None;
    }
    let mut j = i + 1;
    if word(j, b"CURRENT") || word(j, b"STACKED") {
        j += 1;
    }
    if !word(j, b"DIAGNOSTICS") {
        return None;
    }
    j += 1;
    let end = (j..toks.len())
        .find(|&k| punct(k, b";"))
        .unwrap_or(toks.len());
    if end == j {
        return None;
    }
    let mut targets = Vec::new();
    let mut seg = j;
    while seg < end {
        let stop = (seg..end).find(|&k| punct(k, b",")).unwrap_or(end);
        // `target = ITEM`, after `CONDITION n` in the first segment.
        let eq = (seg..stop).find(|&k| punct(k, b"="))?;
        if eq == seg || eq + 2 != stop {
            return None;
        }
        let item = toks[eq + 1];
        if item.kind != TokenKind::Word {
            return None;
        }
        let mut from = eq - 1;
        // `@ name`: a bare `@` before the name.
        if from > seg
            && toks[from - 1].kind == TokenKind::UserVar
            && toks[from - 1].bytes(src) == b"@"
        {
            from -= 1;
        }
        let target = &src[toks[from].start as usize..toks[eq - 1].end as usize];
        if !NUMERIC_ITEMS
            .iter()
            .any(|n| item.bytes(src).eq_ignore_ascii_case(n))
        {
            targets.push(String::from_utf8_lossy(target).into_owned());
        }
        seg = stop + 1;
    }
    if targets.is_empty() {
        return None;
    }
    Some(Diagnostics {
        last: end - 1,
        targets,
    })
}

/// The mapping statements after a `GET DIAGNOSTICS`: `; SET t =
/// <map>(t)` for each target.
fn mapping_sets(targets: &[String]) -> String {
    let mut out = String::new();
    for t in targets {
        out.push_str(GD_SET);
        out.push_str(t);
        out.push_str(" = ");
        out.push_str(MAP_REF);
        out.push('(');
        out.push_str(t);
        out.push(')');
    }
    out
}

/// The length of the mappings and block end [`substitute_with`] puts
/// after a `GET DIAGNOSTICS` ending at byte `at` (`; SET t =
/// <map>(t)` one or more times, then `; END`), if `text` has them there.
fn mapping_len(text: &[u8], at: usize) -> Option<usize> {
    let call = format!(" = {MAP_REF}(");
    let mut k = at;
    let mut sets = 0;
    loop {
        let rest = text.get(k..)?;
        if sets > 0 && rest.starts_with(GD_CLOSE.as_bytes()) {
            return Some(k + GD_CLOSE.len() - at);
        }
        if !rest.starts_with(GD_SET.as_bytes()) {
            return None;
        }
        k += GD_SET.len();
        let m = k + text[k..]
            .windows(call.len())
            .position(|w| w == call.as_bytes())?;
        let target = &text[k..m];
        k = m + call.len();
        if target.is_empty()
            || !text[k..].starts_with(target)
            || text.get(k + target.len()) != Some(&b')')
        {
            return None;
        }
        k += target.len() + 1;
        sets += 1;
    }
}

/// The edits that map caught error text in a body: a block around each
/// `GET DIAGNOSTICS` with text items, and, with `oracle`, each bare
/// `SQLERRM` through [`MAP_FUNCTION`]. Text already mapped is left alone.
fn diagnostics_edits(src: &[u8], toks: &[Token], oracle: bool) -> Vec<(Range<usize>, String)> {
    let mut edits = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        if t.kind != TokenKind::Word {
            i += 1;
            continue;
        }
        let b = t.bytes(src);
        if b.eq_ignore_ascii_case(b"GET")
            && let Some(d) = diagnostics_at(src, toks, i)
        {
            let start = t.start as usize;
            let end = toks[d.last].end as usize;
            // Already mapped (a re-creation from physical text).
            if mapping_len(src, end).is_none() {
                edits.push((start..start, GD_OPEN.to_owned()));
                let mut tail = mapping_sets(&d.targets);
                tail.push_str(GD_CLOSE);
                edits.push((end..end, tail));
            }
            i = d.last + 1;
            continue;
        }
        if oracle && b.eq_ignore_ascii_case(b"SQLERRM") {
            let p = |j: usize, c: &[u8]| {
                toks.get(j).is_some_and(|x| {
                    matches!(x.kind, TokenKind::Punct | TokenKind::Op) && x.bytes(src) == c
                })
            };
            let qualified = i > 0 && p(i - 1, b".");
            let called = p(i + 1, b"(") || p(i + 1, b".");
            let mapped = i > 1
                && p(i - 1, b"(")
                && toks[i - 2].kind == TokenKind::QuotedIdent
                && toks[i - 2].bytes(src) == format!("`{MAP_FUNCTION}`").as_bytes();
            if !qualified && !called && !mapped {
                let text = String::from_utf8_lossy(b);
                edits.push((
                    t.start as usize..t.end as usize,
                    format!("{MAP_REF}({text})"),
                ));
            }
        }
        i += 1;
    }
    edits
}

/// A client's own statement with caught error text mapped (chapter 07
/// step 12), or `None` when nothing changes:
///
/// - `GET DIAGNOSTICS` with text items into user variables runs inside
///   `BEGIN NOT ATOMIC ... END` with its mapping (a compound statement
///   doesn't clear the diagnostics area; it doesn't add the 1758 of a
///   condition number past the last condition either);
/// - a compound statement (`BEGIN NOT ATOMIC`, `IF`, `CASE`, `LOOP`,
///   `WHILE`, `REPEAT` at the top level) gets [`substitute`]'s blocks, and
///   with `oracle` its `SQLERRM` mapped;
/// - the string of `PREPARE ... FROM '...'` and `EXECUTE IMMEDIATE '...'`
///   gets the same, as a statement of its own (MariaDB prepares compound
///   statements).
///
/// Account functions are left to the statement rewrite. Text from a
/// variable (`PREPARE s FROM @q`) is out of reach.
#[must_use]
pub fn top_level(sql: &[u8], lex_opts: &LexOptions, oracle: bool) -> Option<Vec<u8>> {
    let has = |w: &[u8]| sql.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
    if !(has(b"DIAGNOSTICS") || (oracle && has(b"SQLERRM"))) {
        return None;
    }
    let opts = lex_opts.version(999_999);
    let toks: Vec<Token> = lex(sql, &opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let first = toks.first()?;
    let word = |j: usize, w: &[u8]| {
        toks.get(j)
            .is_some_and(|t| t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w))
    };
    if word(0, b"GET") {
        let d = diagnostics_at(sql, &toks, 0)?;
        let end = toks[d.last].end as usize;
        let mut out = b"BEGIN NOT ATOMIC ".to_vec();
        out.extend_from_slice(&sql[first.start as usize..end]);
        out.extend_from_slice(mapping_sets(&d.targets).as_bytes());
        out.extend_from_slice(b"; END");
        out.extend_from_slice(&sql[end..]);
        let mut whole = sql[..first.start as usize].to_vec();
        whole.extend_from_slice(&out);
        return Some(whole);
    }
    // The string of `PREPARE s FROM '...'` or `EXECUTE IMMEDIATE '...'`.
    let lit = if word(0, b"PREPARE") && word(2, b"FROM") {
        Some(3)
    } else if word(0, b"EXECUTE") && word(1, b"IMMEDIATE") {
        Some(2)
    } else {
        None
    };
    if let Some(k) = lit {
        let t = toks.get(k)?;
        let tok = t.bytes(sql);
        let prefix = match t.kind {
            TokenKind::String => 0,
            TokenKind::NationalString => 1,
            _ => return None,
        };
        let nbe = lex_opts.no_backslash_escapes;
        let inner = crate::lex::string_value(&tok[prefix..], nbe)?;
        let mapped = top_level(&inner, lex_opts, oracle)?;
        let mapped = String::from_utf8(mapped).ok()?;
        let start = t.start as usize + prefix;
        let mut out = sql[..start].to_vec();
        out.extend_from_slice(super::rewrite::sql_string(&mapped, nbe).as_bytes());
        out.extend_from_slice(&sql[t.end as usize..]);
        return Some(out);
    }
    apply(sql, diagnostics_edits(sql, &toks, oracle))
}

/// The byte ranges of the lookups in `text`: each `( SELECT ... )` that
/// names [`NAMES_VIEW`] and holds no other `( SELECT`. Quotes (`'`, `"`,
/// `` ` ``) are skipped, with doubled quotes and backslash escapes.
fn groups(text: &[u8]) -> Vec<Range<usize>> {
    if !text.windows(MARKER.len()).any(|w| w == MARKER) {
        return Vec::new();
    }
    // Every `( SELECT` outside quotes, with the index of its `)`.
    let mut opens: Vec<usize> = Vec::new();
    let mut spans: Vec<Range<usize>> = Vec::new();
    let mut stack: Vec<(usize, bool)> = Vec::new();
    let mut i = 0;
    while i < text.len() {
        match text[i] {
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < text.len() {
                    if text[i] == b'\\' && q != b'`' {
                        i += 2;
                        continue;
                    }
                    if text[i] == q {
                        if text.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'(' => {
                let mut j = i + 1;
                while j < text.len() && text[j].is_ascii_whitespace() {
                    j += 1;
                }
                let sel = text.len() >= j + 6
                    && text[j..j + 6].eq_ignore_ascii_case(b"SELECT")
                    && text
                        .get(j + 6)
                        .is_none_or(|c| !c.is_ascii_alphanumeric() && *c != b'_');
                if sel {
                    opens.push(i);
                }
                stack.push((i, sel));
                i += 1;
            }
            b')' => {
                if let Some((o, true)) = stack.pop() {
                    spans.push(o..i + 1);
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    spans
        .into_iter()
        .filter(|s| {
            let body = &text[s.clone()];
            body.windows(MARKER.len()).any(|w| w == MARKER)
                && !opens.iter().any(|&o| o > s.start && o < s.end)
        })
        .collect()
}

/// The call a lookup stands for, as written inside it (`current_user()`
/// in MariaDB's canonical view text, `CURRENT_USER()` in a routine's).
fn call_in(group: &[u8]) -> Option<String> {
    let names: [&[u8]; 7] = [
        b"CURRENT_USER(",
        b"SESSION_USER(",
        b"SYSTEM_USER(",
        b"DATABASE(",
        b"SCHEMA(",
        b"USER(",
        b"VERSION(",
    ];
    let mut best: Option<(usize, usize)> = None;
    for n in names {
        let mut k = 0;
        while k + n.len() <= group.len() {
            if group[k..k + n.len()].eq_ignore_ascii_case(n)
                && (k == 0 || !(group[k - 1].is_ascii_alphanumeric() || group[k - 1] == b'_'))
            {
                if best.is_none_or(|(p, _)| k < p) {
                    best = Some((k, n.len()));
                }
                break;
            }
            k += 1;
        }
    }
    let (p, len) = best?;
    let name = String::from_utf8_lossy(&group[p..p + len - 1]).into_owned();
    Some(format!("{name}()"))
}

/// `text` with every lookup [`substitute`] made turned back into its call.
/// An alias it added stays (`` CURRENT_USER() AS `CURRENT_USER()` ``), as
/// MySQL prints a view's generated column names. Text without lookups is
/// returned as it is. The blocks and `SQLERRM` mappings of caught error
/// text ([`substitute_with`]) are removed too.
#[must_use]
pub fn restore(text: &[u8]) -> Cow<'_, [u8]> {
    if !has_lookup(text) {
        return Cow::Borrowed(text);
    }
    let unwrapped = unwrap_diagnostics(text);
    let base: &[u8] = unwrapped.as_deref().unwrap_or(text);
    let calls: Option<Vec<u8>> = match restore_groups(base) {
        Cow::Owned(o) => Some(o),
        Cow::Borrowed(_) => None,
    };
    match (calls, unwrapped) {
        (Some(o), _) | (None, Some(o)) => Cow::Owned(o),
        (None, None) => Cow::Borrowed(text),
    }
}

fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

/// The position of `pat` in `text` at or after `from`, outside quotes and
/// plain comments (an executable comment's body is searched).
fn find_outside(text: &[u8], from: usize, pat: &[u8]) -> Option<usize> {
    let mut i = from;
    while i < text.len() {
        if text[i..].starts_with(pat) {
            return Some(i);
        }
        match text[i] {
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < text.len() {
                    if text[i] == b'\\' && q != b'`' {
                        i += 2;
                        continue;
                    }
                    if text[i] == q {
                        if text.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'/' if text.get(i + 1) == Some(&b'*')
                && !matches!(text.get(i + 2), Some(b'!' | b'M' | b'+')) =>
            {
                i = text[i + 2..]
                    .windows(2)
                    .position(|w| w == b"*/")
                    .map_or(text.len(), |p| i + 2 + p + 2);
            }
            _ => i += 1,
        }
    }
    None
}

/// `text` without the blocks around `GET DIAGNOSTICS` and the `SQLERRM`
/// mappings [`substitute_with`] made; `None` when it has none.
fn unwrap_diagnostics(text: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len());
    let mut copied = 0;
    let mut pos = 0;
    let mut changed = false;
    // `BEGIN GET ...; SET t = <map>(t)...; END`: the statement holds no
    // `;` outside quotes and comments, so its end is the first one.
    while let Some(open) = find_outside(text, pos, GD_OPEN.as_bytes()) {
        let body = open + GD_OPEN.len();
        pos = open + 1;
        let starts_get = text
            .get(body..body + 4)
            .is_some_and(|w| w[..3].eq_ignore_ascii_case(b"GET") && !is_ident_byte(w[3]));
        if !starts_get || (open > 0 && is_ident_byte(text[open - 1])) {
            continue;
        }
        let Some(end) = find_outside(text, body, b";") else {
            break;
        };
        let Some(len) = mapping_len(text, end) else {
            continue;
        };
        out.extend_from_slice(&text[copied..open]);
        out.extend_from_slice(&text[body..end]);
        copied = end + len;
        pos = copied;
        changed = true;
    }
    out.extend_from_slice(&text[copied..]);
    // `<map>(SQLERRM)`, as written.
    let call = format!("{MAP_REF}(");
    let mut pos = 0;
    let mut again = Vec::with_capacity(out.len());
    let mut copied = 0;
    while let Some(at) = find_outside(&out, pos, call.as_bytes()) {
        let arg = at + call.len();
        let word = out
            .get(arg..arg + 7)
            .filter(|w| w.eq_ignore_ascii_case(b"SQLERRM"));
        if let Some(w) = word
            && out.get(arg + 7) == Some(&b')')
        {
            again.extend_from_slice(&out[copied..at]);
            again.extend_from_slice(w);
            copied = arg + 8;
            changed = true;
        }
        pos = arg;
    }
    again.extend_from_slice(&out[copied..]);
    changed.then_some(again)
}

/// [`restore`]'s lookups part.
fn restore_groups(text: &[u8]) -> Cow<'_, [u8]> {
    let gs = groups(text);
    if gs.is_empty() {
        return Cow::Borrowed(text);
    }
    let mut out = Vec::with_capacity(text.len());
    let mut copied = 0;
    for g in gs {
        let Some(call) = call_in(&text[g.clone()]) else {
            continue;
        };
        out.extend_from_slice(&text[copied..g.start]);
        out.extend_from_slice(call.as_bytes());
        copied = g.end;
    }
    out.extend_from_slice(&text[copied..]);
    Cow::Owned(out)
}

/// [`restore`] on a string.
#[must_use]
pub fn restore_str(text: &str) -> Cow<'_, str> {
    match restore(text.as_bytes()) {
        Cow::Borrowed(_) => Cow::Borrowed(text),
        Cow::Owned(o) => Cow::Owned(String::from_utf8_lossy(&o).into_owned()),
    }
}

/// Whether `text` holds a lookup or a mapping of caught error text.
#[must_use]
pub fn has_lookup(text: &[u8]) -> bool {
    text.windows(ANY_MARKER.len()).any(|w| w == ANY_MARKER)
}

#[cfg(test)]
mod tests;
